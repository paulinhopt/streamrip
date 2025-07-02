import asyncio
import logging
import os
from dataclasses import dataclass

from .. import progress
from ..client import Client
from ..config import Config
from ..db import Database
from ..exceptions import NonStreamableError # Import APIError, NetworkError if needed for type hints
from ..filepath_utils import clean_filepath
from ..metadata import AlbumMetadata
from ..metadata.util import get_album_track_ids
from .artwork import download_artwork
from .media import Media, Pending
from .track import PendingTrack

logger = logging.getLogger("streamrip")


@dataclass(slots=True)
class Album(Media):
    meta: AlbumMetadata
    tracks: list[PendingTrack]
    config: Config
    # folder where the tracks will be downloaded
    folder: str
    db: Database

    async def preprocess(self):
        progress.add_title(self.meta.album)

    async def download(self):
        async def _resolve_and_download(pending: Pending):
            track_id_for_log = getattr(pending, 'id', 'unknown_pending_track')
            logger.debug(f"Album._resolve_and_download: Processing pending track ID {track_id_for_log}")
            try:
                track = await pending.resolve()
                if track is None:
                    logger.debug(f"Album._resolve_and_download: Pending track ID {track_id_for_log} resolved to None. Skipping rip.")
                    return
                logger.debug(f"Album._resolve_and_download: Pending track ID {track_id_for_log} resolved to track. Calling rip.")
                await track.rip()
                logger.debug(f"Album._resolve_and_download: Rip completed for track from pending ID {track_id_for_log}.")
            except Exception as e:
                logger.error(
                    f"Album._resolve_and_download: Error processing pending track "
                    f"ID {track_id_for_log}. Type: {type(e)}, Repr: {repr(e)}, Args: {e.args}. Exception: {e}",
                    exc_info=True
                )

        results = await asyncio.gather(
            *[_resolve_and_download(p) for p in self.tracks], return_exceptions=True
        )

        for i, result_or_exc in enumerate(results):
            if isinstance(result_or_exc, Exception):
                track_id_for_log = "unknown_track_id"
                try:
                    if i < len(self.tracks) and hasattr(self.tracks[i], 'id'):
                        track_id_for_log = self.tracks[i].id
                except Exception:
                    pass
                logger.error(
                    f"Album.download: Unhandled exception from gather for track {track_id_for_log} (index {i}): "
                    f"Type: {type(result_or_exc)}, Repr: {repr(result_or_exc)}, Args: {result_or_exc.args}. Exception: {result_or_exc}",
                    exc_info=True
                )

    async def postprocess(self):
        progress.remove_title(self.meta.album)


@dataclass(slots=True)
class PendingAlbum(Pending):
    id: str
    client: Client
    config: Config
    db: Database

    async def resolve(self) -> Album | None:
        logger.debug(f"PendingAlbum.resolve: Attempting to get metadata for album ID {self.id} from {self.client.source}")
        try:
            resp = await self.client.get_metadata(self.id, "album")
            logger.debug(f"PendingAlbum.resolve: Got metadata for album ID {self.id}. Type: {type(resp)}, Is None: {resp is None}")
        except NonStreamableError as e: # Already specific, keep as warning or error based on severity
            logger.warning(
                f"PendingAlbum.resolve: Album {self.id} on {self.client.source} not streamable (metadata): {e.get_display_message() if hasattr(e, 'get_display_message') else e}. Returning None."
            )
            return None
        except (NetworkError, APIError) as e: # streamrip.exceptions.APIError
            logger.error(f"PendingAlbum.resolve: API/Network error fetching metadata for album {self.id} on {self.client.source}: {e.get_display_message() if hasattr(e, 'get_display_message') else e}. Returning None.")
            return None
        except Exception as e:
            logger.error(f"PendingAlbum.resolve: Unexpected error fetching metadata for album {self.id} on {self.client.source}: {e}. Returning None.", exc_info=True)
            return None

        if resp is None:
            logger.error(f"PendingAlbum.resolve: No metadata returned for album {self.id} on {self.client.source} (resp is None). Returning None.")
            return None

        meta = None
        try:
            logger.debug(f"PendingAlbum.resolve: Building AlbumMetadata for ID {self.id}")
            meta = AlbumMetadata.from_album_resp(resp, self.client.source)
            logger.debug(f"PendingAlbum.resolve: Built AlbumMetadata for ID {self.id}. Is None: {meta is None}")
        except Exception as e:
            logger.error(f"PendingAlbum.resolve: Error building album metadata for {self.id} from response (type: {type(resp)}): {e}. Returning None.", exc_info=True)
            return None

        if meta is None:
            logger.error(
                f"PendingAlbum.resolve: Failed to build AlbumMetadata for {self.id} on {self.client.source} (meta is None). Returning None."
            )
            return None

        tracklist = get_album_track_ids(self.client.source, resp)
        if not tracklist:
            logger.warning(f"PendingAlbum.resolve: No tracks found in metadata for album {self.id} on {self.client.source}.")
            # Decide if an album with no tracks is an error or just an empty album
            # For now, proceed to create an Album object, it will just have no tracks to download.

        folder = self.config.session.downloads.folder
        album_folder = self._album_folder(folder, meta)
        try:
            os.makedirs(album_folder, exist_ok=True)
        except OSError as e:
            logger.error(f"PendingAlbum.resolve: Could not create album folder {album_folder}: {e}. Returning None.", exc_info=True)
            return None

        embed_cover = None
        try:
            embed_cover, _ = await download_artwork(
                self.client.session, # Pass client's aiohttp session
                album_folder,
                meta.covers,
                self.config.session.artwork,
                for_playlist=False,
            )
        except Exception as e:
            logger.error(f"PendingAlbum.resolve: Error downloading artwork for album {self.id}: {e}. Proceeding without cover.", exc_info=True)
            # Continue without cover if artwork download fails

        pending_tracks = [
            PendingTrack(
                track_id, # Use track_id from tracklist
                album=meta, # Pass the resolved AlbumMetadata
                client=self.client,
                config=self.config,
                folder=album_folder, # Pass the determined album folder
                db=self.db,
                cover_path=embed_cover, # Pass path to downloaded cover
            )
            for track_id in tracklist # Iterate over IDs from get_album_track_ids
        ]
        logger.debug(f"PendingAlbum.resolve: Created {len(pending_tracks)} pending tracks for album {self.id}.")
        return Album(meta, pending_tracks, self.config, album_folder, self.db)

    def _album_folder(self, parent: str, meta: AlbumMetadata) -> str:
        config = self.config.session
        if config.downloads.source_subdirectories:
            parent = os.path.join(parent, self.client.source.capitalize())

        # Ensure meta.format_folder_path is called correctly
        # It should use attributes from the meta object (AlbumMetadata)
        # Example: meta.albumartist, meta.album, meta.year
        try:
            album_folder_name = meta.format_folder_path(formatter=config.filepaths.folder_format)
        except Exception as e:
            logger.error(f"Error formatting album folder name for album ID {meta.id if hasattr(meta, 'id') else 'unknown'}: {e}. Using default.", exc_info=True)
            # Fallback to a simple name if formatting fails
            album_folder_name = f"{meta.album_artist_display()} - {meta.album_display()}"


        folder = clean_filepath(
            album_folder_name,
            config.filepaths.restrict_characters
        )

        return os.path.join(parent, folder)

from ..exceptions import NetworkError, APIError # Ensure these are available if not already imported at top
