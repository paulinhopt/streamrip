import asyncio
import logging
import os
from dataclasses import dataclass

from .. import converter
from ..client import Client, Downloadable
from ..config import Config
from ..db import Database
from ..exceptions import NonStreamableError, NetworkError, APIError, InvalidAPIResponseError # Added InvalidAPIResponseError
from ..filepath_utils import clean_filename
from ..metadata import AlbumMetadata, Covers, TrackMetadata, tag_file
from ..progress import add_title, get_progress_callback, remove_title
from .artwork import download_artwork
from .media import Media, Pending
from .semaphore import global_download_semaphore

logger = logging.getLogger("streamrip")


@dataclass(slots=True)
class Track(Media):
    meta: TrackMetadata
    downloadable: Downloadable
    config: Config
    folder: str
    # Is None if a cover doesn't exist for the track
    cover_path: str | None
    db: Database
    download_path: str = ""
    is_single: bool = False

    async def preprocess(self):
        self._set_download_path()
        try:
            os.makedirs(self.folder, exist_ok=True)
        except OSError as e:
            logger.error(f"Track.preprocess: Could not create track folder {self.folder} for '{self.meta.title}': {e}", exc_info=True)
            # Decide if this is a fatal error for this track. If so, raise an exception or handle.
            # For now, let it proceed, download might fail if folder doesn't exist.
        if self.is_single:
            add_title(self.meta.title)

    async def download(self):
        logger.debug(f"Track.download: Starting download for '{self.meta.title}' (ID: {self.meta.info.id}) to {self.download_path}")
        if not self.downloadable or not hasattr(self.downloadable, 'download'):
            logger.error(f"Track.download: No valid downloadable object for track '{self.meta.title}' (ID: {self.meta.info.id}). Skipping download.")
            self.db.set_failed(self.meta.info.source if hasattr(self.meta.info, 'source') else 'unknown_source', "track", self.meta.info.id)
            return

        retry = False
        try:
            async with global_download_semaphore(self.config.session.downloads):
                logger.debug(f"Track.download: Semaphore acquired for '{self.meta.title}'")
                download_size = await self.downloadable.size() # Get size before callback
                with get_progress_callback(
                    self.config.session.cli.progress_bars,
                    download_size, # Use pre-fetched size
                    f"Track {self.meta.tracknumber} ({self.meta.title_version_display()})",
                ) as callback:
                    logger.debug(f"Track.download: Attempting download for '{self.meta.title}'")
                    await self.downloadable.download(self.download_path, callback)
                    logger.debug(f"Track.download: Initial download successful for '{self.meta.title}'")

        except Exception as e:
            logger.error(
                f"Error during initial download of track '{self.meta.title}' (ID: {self.meta.info.id}): "
                f"Type: {type(e)}, Repr: {repr(e)}. Retrying...",
                exc_info=True
            )
            retry = True

        if not retry:
            return

        logger.info(f"Retrying download for track '{self.meta.title}' (ID: {self.meta.info.id})")
        try:
            async with global_download_semaphore(self.config.session.downloads):
                logger.debug(f"Track.download: Semaphore acquired for retry of '{self.meta.title}'")
                download_size = await self.downloadable.size() # Get size again for retry
                with get_progress_callback(
                    self.config.session.cli.progress_bars,
                    download_size,
                    f"Track {self.meta.tracknumber} ({self.meta.title_version_display()}) (retry)",
                ) as callback:
                    logger.debug(f"Track.download: Attempting retry download for '{self.meta.title}'")
                    await self.downloadable.download(self.download_path, callback)
                    logger.info(f"Track.download: Retry download successful for '{self.meta.title}'")
        except Exception as e:
            logger.error(
                f"Persistent error downloading track '{self.meta.title}' (ID: {self.meta.info.id}) after retry: "
                f"Type: {type(e)}, Repr: {repr(e)}. Skipping.",
                exc_info=True
            )
            # Ensure downloadable.source and meta.info.id are valid before using
            source = self.downloadable.source if hasattr(self.downloadable, 'source') else 'unknown_source'
            track_id_to_fail = self.meta.info.id if hasattr(self.meta.info, 'id') else 'unknown_id'
            self.db.set_failed(source, "track", track_id_to_fail)


    async def postprocess(self):
        if self.is_single:
            remove_title(self.meta.title)

        if not os.path.exists(self.download_path):
            logger.error(f"Track.postprocess: Download path {self.download_path} does not exist for tagging/conversion. Skipping postprocessing for '{self.meta.title}'.")
            # Potentially mark as failed if file doesn't exist after download claims success
            # self.db.set_failed(self.meta.info.source, "track", self.meta.info.id) # Be cautious here
            return

        try:
            await tag_file(self.download_path, self.meta, self.cover_path)
        except Exception as e:
            logger.error(f"Track.postprocess: Error tagging file {self.download_path} for '{self.meta.title}': {e}", exc_info=True)
            # Decide if this is a critical error to stop or just log

        if self.config.session.conversion.enabled:
            try:
                await self._convert()
            except Exception as e:
                logger.error(f"Track.postprocess: Error converting file {self.download_path} for '{self.meta.title}': {e}", exc_info=True)
                # If conversion fails, the original (downloaded) file still exists unless _convert deletes it prematurely.
                # The self.download_path might be outdated if conversion failed mid-way.

        # Only mark as downloaded if all critical steps (download) are successful.
        # Tagging/conversion failures might be logged but not necessarily prevent marking as downloaded.
        # This depends on desired behavior. For now, assume download success is key.
        if os.path.exists(self.download_path): # Check if final file (original or converted) exists
             self.db.set_downloaded(self.meta.info.id)
        else:
            logger.warning(f"Track.postprocess: Final file for '{self.meta.title}' (ID: {self.meta.info.id}) not found at {self.download_path} after postprocessing. Not marking as downloaded.")


    async def _convert(self):
        c = self.config.session.conversion
        engine_class = converter.get(c.codec)
        # Ensure self.download_path exists before attempting conversion
        if not os.path.exists(self.download_path):
            logger.error(f"Track._convert: Source file {self.download_path} for conversion does not exist. Skipping conversion for '{self.meta.title}'.")
            return

        engine = engine_class(
            filename=self.download_path,
            sampling_rate=c.sampling_rate,
            bit_depth=c.bit_depth,
            remove_source=True,
        )
        logger.debug(f"Track._convert: Converting '{self.meta.title}' to {c.codec.upper()}")
        await engine.convert()
        logger.info(f"Track._convert: Conversion successful for '{self.meta.title}'. New path: {engine.final_fn}")
        self.download_path = engine.final_fn

    def _set_download_path(self):
        c = self.config.session.filepaths
        formatter = c.track_format

        try:
            track_filename_part = self.meta.format_track_path(formatter)
        except Exception as e:
            logger.error(f"Error formatting track path for track ID {self.meta.info.id}: {e}. Using default.", exc_info=True)
            track_filename_part = f"{self.meta.tracknumber_display()} - {self.meta.title_display()}"


        track_path = clean_filename(
            track_filename_part,
            restrict=c.restrict_characters,
        )
        if c.truncate_to > 0 and len(track_path) > c.truncate_to:
            track_path = track_path[: c.truncate_to]

        # Ensure downloadable and its extension are valid before forming path
        extension = "unknown"
        if self.downloadable and hasattr(self.downloadable, 'extension') and self.downloadable.extension:
            extension = self.downloadable.extension
        else:
            logger.warning(f"Track._set_download_path: Downloadable or its extension is invalid for track '{self.meta.title}'. Using '.unknown' extension.")


        self.download_path = os.path.join(
            self.folder,
            f"{track_path}.{extension}",
        )
        logger.debug(f"Track._set_download_path: Set download path for '{self.meta.title}' to {self.download_path}")


@dataclass(slots=True)
class PendingTrack(Pending):
    id: str
    album: AlbumMetadata
    client: Client
    config: Config
    folder: str
    db: Database
    cover_path: str | None

    async def resolve(self) -> Track | None:
        logger.debug(f"PendingTrack.resolve: Starting for track ID {self.id}")
        if self.db.downloaded(self.id):
            logger.info(
                f"PendingTrack.resolve: Skipping track {self.id}. Marked as downloaded in the database."
            )
            return None

        source = self.client.source
        resp = None
        logger.debug(f"PendingTrack.resolve: Attempting to get metadata for track ID {self.id} from {source}")
        try:
            resp = await self.client.get_metadata(self.id, "track")
            logger.debug(f"PendingTrack.resolve: Got metadata for track ID {self.id}. Type: {type(resp)}, Is None: {resp is None}")
        except NonStreamableError as e:
            logger.warning(f"PendingTrack.resolve: Track {self.id} on {source} not streamable (metadata): {e.get_display_message() if hasattr(e, 'get_display_message') else e}. Returning None.")
            self.db.set_failed(source, "track", self.id)
            return None
        except (NetworkError, APIError) as e:
            logger.error(f"PendingTrack.resolve: API/Network error fetching metadata for track {self.id} on {source}: {e.get_display_message() if hasattr(e, 'get_display_message') else e}. Returning None.")
            self.db.set_failed(source, "track", self.id)
            return None
        except Exception as e:
            logger.error(f"PendingTrack.resolve: Unexpected error fetching metadata for track {self.id} on {source}: {e}. Returning None.", exc_info=True)
            self.db.set_failed(source, "track", self.id)
            return None

        if resp is None:
            logger.error(f"PendingTrack.resolve: No metadata returned for track {self.id} on {source} (resp is None). Returning None.")
            self.db.set_failed(source, "track", self.id)
            return None

        meta = None
        try:
            logger.debug(f"PendingTrack.resolve: Building TrackMetadata for ID {self.id}")
            meta = TrackMetadata.from_resp(self.album, source, resp)
            logger.debug(f"PendingTrack.resolve: Built TrackMetadata for ID {self.id}. Is None: {meta is None}")
        except Exception as e:
            logger.error(f"PendingTrack.resolve: Error building track metadata for {self.id}. Response type: {type(resp)}. Error: {e}. Returning None.", exc_info=True)
            self.db.set_failed(source, "track", self.id)
            return None

        if meta is None:
            logger.error(f"PendingTrack.resolve: Failed to build TrackMetadata for {self.id} on {source} (meta is None). Returning None.")
            self.db.set_failed(source, "track", self.id)
            return None

        quality = self.config.session.get_source(source).quality
        downloadable = None
        logger.debug(f"PendingTrack.resolve: Attempting to get downloadable for track ID {self.id}, title '{meta.title}', quality {quality}")
        try:
            downloadable = await self.client.get_downloadable(self.id, quality)
            logger.debug(f"PendingTrack.resolve: Got downloadable for track ID {self.id}. Type: {type(downloadable)}, Is None: {downloadable is None}")
        except NonStreamableError as e:
            logger.warning(
                f"PendingTrack.resolve: Track {meta.title} ({self.id}) on {source} not streamable (downloadable): {e.get_display_message() if hasattr(e, 'get_display_message') else e}. Returning None."
            )
            self.db.set_failed(source, "track", self.id)
            return None
        except (NetworkError, APIError) as e:
            logger.error(f"PendingTrack.resolve: API/Network error fetching downloadable for track {meta.title} ({self.id}) on {source}: {e.get_display_message() if hasattr(e, 'get_display_message') else e}. Returning None.")
            self.db.set_failed(source, "track", self.id)
            return None
        except Exception as e:
            logger.error(f"PendingTrack.resolve: Unexpected error fetching downloadable for track {meta.title} ({self.id}) on {source}: {e}. Returning None.", exc_info=True)
            self.db.set_failed(source, "track", self.id)
            return None

        if downloadable is None:
            logger.error(f"PendingTrack.resolve: No downloadable returned for track {meta.title} ({self.id}) on {source} (downloadable is None). Returning None.")
            self.db.set_failed(source, "track", self.id)
            return None

        downloads_config = self.config.session.downloads
        current_folder = self.folder
        if downloads_config.disc_subdirectories and hasattr(self.album, 'disctotal') and self.album.disctotal > 1 and hasattr(meta, 'discnumber'):
            current_folder = os.path.join(self.folder, f"Disc {meta.discnumber}")

        logger.debug(f"PendingTrack.resolve: Successfully resolved track ID {self.id}. Title: '{meta.title}'. Downloadable URL (first 50 chars): {downloadable.url[:50] if hasattr(downloadable, 'url') else 'N/A'}")
        return Track(
            meta=meta, # Use the fully built TrackMetadata
            downloadable=downloadable,
            config=self.config,
            folder=current_folder, # Use potentially modified folder for disc subdirs
            cover_path=self.cover_path,
            db=self.db,
        )


@dataclass(slots=True)
class PendingSingle(Pending):
    id: str
    client: Client
    config: Config
    db: Database

    async def resolve(self) -> Track | None:
        logger.debug(f"PendingSingle.resolve: Starting for track ID {self.id}")
        if self.db.downloaded(self.id):
            logger.info(
                f"PendingSingle.resolve: Skipping track {self.id}. Marked as downloaded."
            )
            return None

        source = self.client.source
        resp = None
        logger.debug(f"PendingSingle.resolve: Getting metadata for track ID {self.id} from {source}")
        try:
            resp = await self.client.get_metadata(self.id, "track")
            logger.debug(f"PendingSingle.resolve: Got metadata for track ID {self.id}. Type: {type(resp)}")
        except NonStreamableError as e:
            logger.warning(f"PendingSingle.resolve: Track {self.id} on {source} not streamable (metadata): {e.get_display_message() if hasattr(e, 'get_display_message') else e}. Returning None.")
            self.db.set_failed(source, "track", self.id)
            return None
        except (NetworkError, APIError) as e:
            logger.error(f"PendingSingle.resolve: API/Network error fetching metadata for track {self.id} on {source}: {e.get_display_message() if hasattr(e, 'get_display_message') else e}. Returning None.")
            self.db.set_failed(source, "track", self.id)
            return None
        except Exception as e:
            logger.error(f"PendingSingle.resolve: Unexpected error fetching metadata for track {self.id} on {source}: {e}. Returning None.", exc_info=True)
            self.db.set_failed(source, "track", self.id)
            return None

        if resp is None:
            logger.error(f"PendingSingle.resolve: No metadata returned for track {self.id} on {source}. Returning None.")
            self.db.set_failed(source, "track", self.id)
            return None

        album_meta = None
        try:
            logger.debug(f"PendingSingle.resolve: Building AlbumMetadata for track ID {self.id}")
            album_meta = AlbumMetadata.from_track_resp(resp, source)
            logger.debug(f"PendingSingle.resolve: Built AlbumMetadata for track ID {self.id}. Is None: {album_meta is None}")
        except Exception as e:
            logger.error(f"PendingSingle.resolve: Error building album metadata for track {self.id}: {e}. Returning None.", exc_info=True)
            self.db.set_failed(source, "track", self.id)
            return None

        if album_meta is None:
            logger.error(f"PendingSingle.resolve: Could not derive album metadata for track {self.id}. Returning None.")
            self.db.set_failed(source, "track", self.id)
            return None

        track_meta = None
        try:
            logger.debug(f"PendingSingle.resolve: Building TrackMetadata for ID {self.id} using derived album.")
            track_meta = TrackMetadata.from_resp(album_meta, source, resp)
            logger.debug(f"PendingSingle.resolve: Built TrackMetadata for ID {self.id}. Is None: {track_meta is None}")
        except Exception as e:
            logger.error(f"PendingSingle.resolve: Error building track metadata for track {self.id}: {e}. Returning None.", exc_info=True)
            self.db.set_failed(source, "track", self.id)
            return None

        if track_meta is None:
            logger.error(f"PendingSingle.resolve: Failed to build track metadata for {self.id}. Returning None.")
            self.db.set_failed(source, "track", self.id)
            return None

        session_config = self.config.session
        quality = getattr(session_config, source).quality

        parent_folder = session_config.downloads.folder
        current_folder = parent_folder
        if session_config.filepaths.add_singles_to_folder:
            current_folder = self._format_folder(album_meta, parent_folder) # Pass parent_folder explicitly

        try:
            os.makedirs(current_folder, exist_ok=True)
        except OSError as e:
            logger.error(f"PendingSingle.resolve: Could not create folder {current_folder} for track {self.id}: {e}. Returning None.", exc_info=True)
            self.db.set_failed(source, "track", self.id) # Mark as failed if folder creation fails
            return None


        embedded_cover_path = None
        downloadable_obj = None
        logger.debug(f"PendingSingle.resolve: Getting cover and downloadable for track ID {self.id}, title '{track_meta.title}'")
        try:
            results = await asyncio.gather(
                self._download_cover(album_meta.covers, current_folder),
                self.client.get_downloadable(self.id, quality),
                return_exceptions=True
            )

            if isinstance(results[0], Exception):
                logger.error(f"PendingSingle.resolve: Error downloading cover for track {self.id}: {results[0]}", exc_info=isinstance(results[0], Exception))
            else:
                embedded_cover_path = results[0]

            if isinstance(results[1], Exception):
                # Log specific error type if it's one of ours, else generic
                err_msg = results[1].get_display_message() if hasattr(results[1], 'get_display_message') else str(results[1])
                logger.error(f"PendingSingle.resolve: Error fetching downloadable for track {self.id}: {err_msg}", exc_info=isinstance(results[1], Exception))
                self.db.set_failed(source, "track", self.id)
                return None
            else:
                downloadable_obj = results[1]
                logger.debug(f"PendingSingle.resolve: Got downloadable for track ID {self.id}. Type: {type(downloadable_obj)}")


        except Exception as e:
            logger.error(f"PendingSingle.resolve: Unexpected error during gather for track {self.id} (cover/downloadable): {e}. Returning None.", exc_info=True)
            self.db.set_failed(source, "track", self.id)
            return None

        if downloadable_obj is None:
            logger.error(f"PendingSingle.resolve: Downloadable is None for track {self.id} after gather. Returning None.")
            self.db.set_failed(source, "track", self.id)
            return None

        logger.debug(f"PendingSingle.resolve: Successfully resolved single track ID {self.id}. Title: '{track_meta.title}'")
        return Track(
            meta=track_meta,
            downloadable=downloadable_obj,
            config=self.config,
            folder=current_folder,
            cover_path=embedded_cover_path,
            db=self.db,
            is_single=True,
        )

    def _format_folder(self, meta: AlbumMetadata, parent_folder_base: str) -> str: # Added parent_folder_base
        c = self.config.session
        current_parent = parent_folder_base # Start with the base downloads folder

        if c.downloads.source_subdirectories:
            current_parent = os.path.join(current_parent, self.client.source.capitalize())

        try:
            album_specific_part = meta.format_folder_path(c.filepaths.folder_format)
        except Exception as e:
            logger.error(f"Error formatting album-specific folder part for album ID {meta.id if hasattr(meta, 'id') else 'unknown'}: {e}. Using default.", exc_info=True)
            album_specific_part = f"{meta.album_artist_display()} - {meta.album_display()}"


        return os.path.join(current_parent, clean_filepath(album_specific_part, c.filepaths.restrict_characters))

    async def _download_cover(self, covers: Covers, folder: str) -> str | None:
        # Ensure client session is available for download_artwork
        if not self.client.session:
            logger.error("PendingSingle._download_cover: Client session not available for downloading cover.")
            # Attempt to initialize session if not available (e.g. if login wasn't called)
            # This is a fallback, ideally session is always ready.
            try:
                await self.client.login() # This might be problematic if called out of sequence
                if not self.client.session: # Still no session after login attempt
                     raise ClientError("Client session could not be initialized for cover download.")
            except Exception as e:
                logger.error(f"Failed to initialize client session for cover download: {e}")
                return None


        logger.debug(f"PendingSingle._download_cover: Attempting to download cover into {folder}")
        embed_path, _ = await download_artwork(
            self.client.session, # Pass the client's aiohttp.ClientSession
            folder,
            covers,
            self.config.session.artwork,
            for_playlist=False, # Assuming singles are not playlist context for artwork
        )
        logger.debug(f"PendingSingle._download_cover: Artwork download result path: {embed_path}")
        return embed_path

from ..exceptions import ClientError, APIError, NetworkError # Ensure these are available if not imported at top
