import asyncio
import logging
import os
from dataclasses import dataclass

from .. import progress
from ..client import Client
from ..config import Config
from ..db import Database
from ..exceptions import NonStreamableError
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
            try:
                track = await pending.resolve()
                if track is None: # PendingTrack.resolve() returned None due to an error it already logged
                    return
                await track.rip() # track is an instance of Track
            except Exception as e: # Catch any unexpected error during track.rip() itself
                # This log should ideally not be common if Track.rip() handles its own errors.
                # However, it's a fallback.
                logger.error(f"Error downloading track (during rip call in album): {e}", exc_info=True)

        results = await asyncio.gather(
            *[_resolve_and_download(p) for p in self.tracks], return_exceptions=True
        )

        # This loop is for exceptions raised by _resolve_and_download coroutine directly
        # if they weren't caught inside it, or if _resolve_and_download itself had an issue
        # not related to the track processing logic (less likely).
        # With return_exceptions=True, `results` will contain exceptions if a coroutine failed.
        for i, result_or_exc in enumerate(results):
            if isinstance(result_or_exc, Exception):
                track_id_for_log = "unknown_track_id"
                try:
                    # Try to get the ID of the track that failed, if self.tracks is still valid
                    # and corresponds to the results list.
                    if i < len(self.tracks) and hasattr(self.tracks[i], 'id'):
                        track_id_for_log = self.tracks[i].id
                except Exception: # Fallback if accessing self.tracks[i].id fails
                    pass
                logger.error(f"Unhandled exception processing track {track_id_for_log} in album: {result_or_exc}", exc_info=True)


    async def postprocess(self):
        progress.remove_title(self.meta.album)


@dataclass(slots=True)
class PendingAlbum(Pending):
    id: str
    client: Client
    config: Config
    db: Database

    async def resolve(self) -> Album | None:
        try:
            resp = await self.client.get_metadata(self.id, "album")
        except NonStreamableError as e:
            logger.error(
                f"Album {self.id} not available to stream on {self.client.source} ({e})",
            )
            return None

        try:
            meta = AlbumMetadata.from_album_resp(resp, self.client.source)
        except Exception as e:
            logger.error(f"Error building album metadata for {id=}: {e}")
            return None

        if meta is None:
            logger.error(
                f"Album {self.id} not available to stream on {self.client.source}",
            )
            return None

        tracklist = get_album_track_ids(self.client.source, resp)
        folder = self.config.session.downloads.folder
        album_folder = self._album_folder(folder, meta)
        os.makedirs(album_folder, exist_ok=True)
        embed_cover, _ = await download_artwork(
            self.client.session,
            album_folder,
            meta.covers,
            self.config.session.artwork,
            for_playlist=False,
        )
        pending_tracks = [
            PendingTrack(
                id,
                album=meta,
                client=self.client,
                config=self.config,
                folder=album_folder,
                db=self.db,
                cover_path=embed_cover,
            )
            for id in tracklist
        ]
        logger.debug("Pending tracks: %s", pending_tracks)
        return Album(meta, pending_tracks, self.config, album_folder, self.db)

    def _album_folder(self, parent: str, meta: AlbumMetadata) -> str:
        config = self.config.session
        if config.downloads.source_subdirectories:
            parent = os.path.join(parent, self.client.source.capitalize())
        formatter = config.filepaths.folder_format
        folder = clean_filepath(
            meta.format_folder_path(formatter), config.filepaths.restrict_characters
        )

        return os.path.join(parent, folder)
