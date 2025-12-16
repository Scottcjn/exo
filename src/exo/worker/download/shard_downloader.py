from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import timedelta
from typing import AsyncIterator, Callable, Self

import anyio
from anyio import Path, create_task_group
from anyio.abc import CancelScope, TaskGroup

from exo.shared.types.memory import Memory
from exo.shared.types.models import ModelId, ModelMetadata
from exo.shared.types.worker.shards import (
    PipelineShardMetadata,
    ShardMetadata,
)
from exo.utils.channels import Receiver, Sender, channel
from exo.worker.download.download_utils import RepoDownloadProgress, download_shard


@dataclass
class ShardDownloader2:
    progress_sender: Sender[RepoDownloadProgress]
    max_parallel_downloads = 8

    # The last item on the shard stack is currently being downloaded
    shard_stack: list[tuple[ShardMetadata, bool]] = field(
        init=False, default_factory=list
    )
    _top_scope: CancelScope | None = field(init=False, default=None)
    _tg: TaskGroup = field(init=False, default_factory=create_task_group)

    def start_shard(self, shard: ShardMetadata, config_only: bool = False):
        self.shard_stack.append((shard, config_only))
        # Cancel current tasks
        if self._top_scope:
            self._top_scope.cancel()
        # Create a new scope
        self._top_scope = CancelScope()

    async def run(self):
        async with self._tg:
            await anyio.sleep_forever()

    def shutdown(self):
        self.progress_sender.close()
        self._tg.cancel_scope.cancel()

    async def _new_download(self, scope: CancelScope):
        (shard, config_only) = self.shard_stack[-1]
        with self.progress_sender.clone() as send, scope:
            allow_patterns = ["config.json"] if config_only else None
            target_dir, _ = await download_shard(
                shard,
                send,
                max_parallel_downloads=self.max_parallel_downloads,
                allow_patterns=allow_patterns,
            )
            return target_dir

    @classmethod
    def default(cls) -> tuple[Self, Receiver[RepoDownloadProgress]]:
        send, recv = channel[RepoDownloadProgress](10)
        return cls(send), recv


# TODO: the PipelineShardMetadata getting reinstantiated is a bit messy. Shoudl this be a classmethod?
class ShardDownloader(ABC):
    @abstractmethod
    async def ensure_shard(
        self, shard: ShardMetadata, config_only: bool = False
    ) -> Path:
        """
        Ensures that the shard is downloaded.
        Does not allow multiple overlapping downloads at once.
        If you try to download a Shard which overlaps a Shard that is already being downloaded,
        the download will be cancelled and a new download will start.

        Args:
            shard (Shard): The shard to download.
        """

    @abstractmethod
    def on_progress(
        self, callback: Callable[[ShardMetadata, RepoDownloadProgress], None]
    ) -> None:
        pass

    @abstractmethod
    async def get_shard_download_status(
        self,
    ) -> AsyncIterator[tuple[Path, RepoDownloadProgress]]:
        """Get the download status of shards.

        Yields:
            tuple[Path, RepoDownloadProgress]: The path and progress of a shard download.
        """
        yield (
            Path("/tmp/noop_shard"),
            RepoDownloadProgress(
                repo_id="noop",
                repo_revision="noop",
                shard=PipelineShardMetadata(
                    model_meta=ModelMetadata(
                        model_id=ModelId("noop"),
                        pretty_name="noope",
                        storage_size=Memory.from_bytes(0),
                        n_layers=1,
                    ),
                    device_rank=0,
                    world_size=1,
                    start_layer=0,
                    end_layer=1,
                    n_layers=1,
                ),
                completed_files=0,
                total_files=0,
                downloaded_bytes=Memory.from_bytes(0),
                downloaded_bytes_this_session=Memory.from_bytes(0),
                total_bytes=Memory.from_bytes(0),
                overall_speed=0,
                overall_eta=timedelta(seconds=0),
                status="complete",
            ),
        )

    @abstractmethod
    async def get_shard_download_status_for_shard(
        self, shard: ShardMetadata
    ) -> RepoDownloadProgress: ...


class NoopShardDownloader(ShardDownloader):
    async def ensure_shard(
        self, shard: ShardMetadata, config_only: bool = False
    ) -> Path:
        return Path("/tmp/noop_shard")

    def on_progress(
        self, callback: Callable[[ShardMetadata, RepoDownloadProgress], None]
    ) -> None:
        pass

    async def get_shard_download_status(
        self,
    ) -> AsyncIterator[tuple[Path, RepoDownloadProgress]]:
        yield (
            Path("/tmp/noop_shard"),
            RepoDownloadProgress(
                repo_id="noop",
                repo_revision="noop",
                shard=PipelineShardMetadata(
                    model_meta=ModelMetadata(
                        model_id=ModelId("noop"),
                        pretty_name="noope",
                        storage_size=Memory.from_bytes(0),
                        n_layers=1,
                    ),
                    device_rank=0,
                    world_size=1,
                    start_layer=0,
                    end_layer=1,
                    n_layers=1,
                ),
                completed_files=0,
                total_files=0,
                downloaded_bytes=Memory.from_bytes(0),
                downloaded_bytes_this_session=Memory.from_bytes(0),
                total_bytes=Memory.from_bytes(0),
                overall_speed=0,
                overall_eta=timedelta(seconds=0),
                status="complete",
            ),
        )

    async def get_shard_download_status_for_shard(
        self, shard: ShardMetadata
    ) -> RepoDownloadProgress:
        return RepoDownloadProgress(
            repo_id="noop",
            repo_revision="noop",
            shard=shard,
            completed_files=0,
            total_files=0,
            downloaded_bytes=Memory.from_bytes(0),
            downloaded_bytes_this_session=Memory.from_bytes(0),
            total_bytes=Memory.from_bytes(0),
            overall_speed=0,
            overall_eta=timedelta(seconds=0),
            status="complete",
        )
