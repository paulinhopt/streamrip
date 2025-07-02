import asyncio
import logging
import os
from dataclasses import dataclass

from .. import converter
from ..client import Client, Downloadable
from ..config import Config
from ..db import Database
from ..exceptions import NonStreamableError
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
    # change?
    download_path: str = ""
    is_single: bool = False

    async def preprocess(self):
        self._set_download_path()
        os.makedirs(self.folder, exist_ok=True)
        if self.is_single:
            add_title(self.meta.title)

    async def download(self):
        # TODO: progress bar description
        async with global_download_semaphore(self.config.session.downloads):
            with get_progress_callback(
                self.config.session.cli.progress_bars,
                await self.downloadable.size(),
                f"Track {self.meta.tracknumber}",
            ) as callback:
                try:
                    await self.downloadable.download(self.download_path, callback)
                    retry = False
                except Exception as e:
                    logger.error(
                        f"Error downloading track '{self.meta.title}', retrying: {e}"
                    )
                    retry = True

            if not retry:
                return

            with get_progress_callback(
                self.config.session.cli.progress_bars,
                await self.downloadable.size(),
                f"Track {self.meta.tracknumber} (retry)",
            ) as callback:
                try:
                    await self.downloadable.download(self.download_path, callback)
                except Exception as e:
                    logger.error(
                        f"Persistent error downloading track '{self.meta.title}', skipping: {e}"
                    )
                    self.db.set_failed(
                        self.downloadable.source, "track", self.meta.info.id
                    )

    async def postprocess(self):
        if self.is_single:
            remove_title(self.meta.title)

        await tag_file(self.download_path, self.meta, self.cover_path)
        if self.config.session.conversion.enabled:
            await self._convert()

        self.db.set_downloaded(self.meta.info.id)

    async def _convert(self):
        c = self.config.session.conversion
        engine_class = converter.get(c.codec)
        engine = engine_class(
            filename=self.download_path,
            sampling_rate=c.sampling_rate,
            bit_depth=c.bit_depth,
            remove_source=True,  # always going to delete the old file
        )
        await engine.convert()
        self.download_path = engine.final_fn  # because the extension changed

    def _set_download_path(self):
        c = self.config.session.filepaths
        formatter = c.track_format
        track_path = clean_filename(
            self.meta.format_track_path(formatter),
            restrict=c.restrict_characters,
        )
        if c.truncate_to > 0 and len(track_path) > c.truncate_to:
            track_path = track_path[: c.truncate_to]

        self.download_path = os.path.join(
            self.folder,
            f"{track_path}.{self.downloadable.extension}",
        )


@dataclass(slots=True)
class PendingTrack(Pending):
    id: str
    album: AlbumMetadata
    client: Client
    config: Config
    folder: str
    db: Database
    # cover_path is None <==> Artwork for this track doesn't exist in API
    cover_path: str | None

    async def resolve(self) -> Track | None:
        if self.db.downloaded(self.id):
            logger.info(
                f"Skipping track {self.id}. Marked as downloaded in the database.",
            )
            return None

        source = self.client.source
        resp = None # Initialize resp
        try:
            resp = await self.client.get_metadata(self.id, "track")
        except NonStreamableError as e:
            logger.warning(f"Track {self.id} on {source} not streamable (metadata): {e.get_display_message() if hasattr(e, 'get_display_message') else e}")
            self.db.set_failed(source, "track", self.id)
            return None
        except (NetworkError, APIError) as e:
            logger.error(f"API/Network error fetching metadata for track {self.id} on {source}: {e.get_display_message() if hasattr(e, 'get_display_message') else e}")
            self.db.set_failed(source, "track", self.id)
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching metadata for track {self.id} on {source}: {e}", exc_info=True)
            self.db.set_failed(source, "track", self.id)
            return None

        if resp is None:
            logger.error(f"No metadata returned for track {self.id} on {source}, but no exception was raised.")
            self.db.set_failed(source, "track", self.id)
            return None

        meta = None # Initialize meta
        try:
            meta = TrackMetadata.from_resp(self.album, source, resp)
        except Exception as e:
            logger.error(f"Error building track metadata for {self.id} from response (type: {type(resp)}): {e}", exc_info=True)
            self.db.set_failed(source, "track", self.id)
            return None

        # This check might be redundant if TrackMetadata.from_resp raises an exception on failure or returns None reliably.
        # If it can return None without an exception, this check is valid.
        if meta is None:
            logger.error(f"Failed to build TrackMetadata for {self.id} on {source} (meta is None).")
            self.db.set_failed(source, "track", self.id)
            return None

        quality = self.config.session.get_source(source).quality
        downloadable = None # Initialize downloadable
        try:
            downloadable = await self.client.get_downloadable(self.id, quality)
        except NonStreamableError as e:
            logger.warning(
                f"Track {meta.title} ({self.id}) on {source} not streamable (downloadable): {e.get_display_message() if hasattr(e, 'get_display_message') else e}"
            )
            self.db.set_failed(source, "track", self.id)
            return None
        except (NetworkError, APIError) as e:
            logger.error(f"API/Network error fetching downloadable for track {meta.title} ({self.id}) on {source}: {e.get_display_message() if hasattr(e, 'get_display_message') else e}")
            self.db.set_failed(source, "track", self.id)
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching downloadable for track {meta.title} ({self.id}) on {source}: {e}", exc_info=True)
            self.db.set_failed(source, "track", self.id)
            return None

        if downloadable is None:
            logger.error(f"No downloadable returned for track {meta.title} ({self.id}) on {source}, but no exception was raised.")
            self.db.set_failed(source, "track", self.id)
            return None

        downloads_config = self.config.session.downloads
        if downloads_config.disc_subdirectories and self.album.disctotal > 1:
            folder = os.path.join(self.folder, f"Disc {meta.discnumber}")
        else:
            folder = self.folder

        return Track(
            meta,
            downloadable,
            self.config,
            folder,
            self.cover_path,
            self.db,
        )


@dataclass(slots=True)
class PendingSingle(Pending):
    """Whereas PendingTrack is used in the context of an album, where the album metadata
    and cover have been resolved, PendingSingle is used when a single track is downloaded.

    This resolves the Album metadata and downloads the cover to pass to the Track class.
    """

    id: str
    client: Client
    config: Config
    db: Database

    async def resolve(self) -> Track | None:
        if self.db.downloaded(self.id):
            logger.info(
                f"Skipping track {self.id}. Marked as downloaded in the database.",
            )
            return None

        try:
            resp = await self.client.get_metadata(self.id, "track")
        except NonStreamableError as e:
            logger.warning(f"Single track {self.id} on {self.client.source} not streamable (metadata): {e.get_display_message() if hasattr(e, 'get_display_message') else e}")
            self.db.set_failed(self.client.source, "track", self.id)
            return None
        except (NetworkError, APIError) as e:
            logger.error(f"API/Network error fetching metadata for single track {self.id} on {self.client.source}: {e.get_display_message() if hasattr(e, 'get_display_message') else e}")
            self.db.set_failed(self.client.source, "track", self.id)
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching metadata for single track {self.id} on {self.client.source}: {e}", exc_info=True)
            self.db.set_failed(self.client.source, "track", self.id)
            return None

        if resp is None:
            logger.error(f"No metadata returned for single track {self.id} on {self.client.source}")
            self.db.set_failed(self.client.source, "track", self.id)
            return None

        album = None
        try:
            # This from_track_resp is crucial; it might be where album data is derived for singles
            album = AlbumMetadata.from_track_resp(resp, self.client.source)
        except Exception as e:
            logger.error(f"Error building album metadata for single track {self.id}: {e}", exc_info=True)
            # Continue without full album data if this fails? Or fail the track?
            # For now, let's allow it to proceed if meta can still be built, but log error.
            # If album is essential for TrackMetadata, this might need to return None.
            # However, TrackMetadata.from_resp takes album as an arg, so it might be an issue.
            # For safety, if album cannot be derived, we should probably fail.
            self.db.set_failed(self.client.source, "track", self.id)
            return None


        if album is None: # Should be caught by the exception above if from_track_resp fails critically
            logger.error(f"Could not derive album metadata for single track {self.id} on {self.client.source}.")
            self.db.set_failed(self.client.source, "track", self.id)
            return None

        meta = None
        try:
            meta = TrackMetadata.from_resp(album, self.client.source, resp)
        except Exception as e:
            logger.error(f"Error building track metadata for single track {self.id}: {e}", exc_info=True)
            self.db.set_failed(self.client.source, "track", self.id)
            return None

        if meta is None: # If from_resp returns None without exception
            logger.error(f"Failed to build track metadata for single track {self.id} on {self.client.source}.")
            self.db.set_failed(self.client.source, "track", self.id)
            return None

        config = self.config.session
        quality = getattr(config, self.client.source).quality
        # assert isinstance(quality, int) # quality is already validated by config loading or Pydantic
        parent = config.downloads.folder
        if config.filepaths.add_singles_to_folder:
            folder = os.path.join(parent, self._format_folder(album))
        else:
            folder = parent

        os.makedirs(folder, exist_ok=True)

        embedded_cover_path = None
        downloadable = None
        try:
            # Gather cover download and track downloadable info concurrently
            results = await asyncio.gather(
                self._download_cover(album.covers, folder),
                self.client.get_downloadable(self.id, quality),
                return_exceptions=True # Handle individual failures
            )

            # Process cover result
            if isinstance(results[0], Exception):
                logger.error(f"Error downloading cover for single track {self.id}: {results[0]}", exc_info=isinstance(results[0], Exception))
                # Continue without cover if it fails
            else:
                embedded_cover_path = results[0]

            # Process downloadable result
            if isinstance(results[1], Exception):
                logger.error(f"Error fetching downloadable for single track {self.id}: {results[1]}", exc_info=isinstance(results[1], Exception))
                self.db.set_failed(self.client.source, "track", self.id)
                return None # Cannot proceed without downloadable
            else:
                downloadable = results[1]

        except Exception as e: # Catch errors from asyncio.gather itself or unexpected issues
            logger.error(f"Unexpected error during gather for single track {self.id} (cover/downloadable): {e}", exc_info=True)
            self.db.set_failed(self.client.source, "track", self.id)
            return None

        if downloadable is None: # Should be caught by gather's exception handling, but as a safeguard
            logger.error(f"Downloadable is None for single track {self.id} after gather, failing.")
            self.db.set_failed(self.client.source, "track", self.id)
            return None

        return Track(
            meta,
            downloadable,
            self.config,
            folder,
            embedded_cover_path,
            self.db,
            is_single=True,
        )

    def _format_folder(self, meta: AlbumMetadata) -> str:
        c = self.config.session
        parent = c.downloads.folder
        formatter = c.filepaths.folder_format
        if c.downloads.source_subdirectories:
            parent = os.path.join(parent, self.client.source.capitalize())

        return os.path.join(parent, meta.format_folder_path(formatter))

    async def _download_cover(self, covers: Covers, folder: str) -> str | None:
        embed_path, _ = await download_artwork(
            self.client.session,
            folder,
            covers,
            self.config.session.artwork,
            for_playlist=False,
        )
        return embed_path
