# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "icechunk>=1.1",
#     "imageio-ffmpeg",
#     "matplotlib",
#     "numpy",
#     "zarr>=3",
# ]
# ///
"""Animate the Icechunk store's time axis growing, commit by commit.

Walks the store's snapshot ancestry on ``main`` and diffs the ``time``
axis between consecutive snapshots, so every granule appears exactly when
its commit landed:

- top panel: each granule at (scan time, commit time) — **append** at the
  axis end, or **out-of-order insert** (held in the pending ledger until a
  re-sort folds it in). Appends hug the diagonal; the backfill and each
  re-sort land as horizontal bands.
- bottom panel: ``len(time)`` — the store's growing time dimension.

Overwrites in place don't move the axis, so they don't appear; expired
snapshots don't either — the replay starts at the oldest one retained.

Defaults to the pipeline's S3 store for the collection, e.g.

    uv run exploration/animate_store_growth.py                  # tempo/hcho/v04
    uv run exploration/animate_store_growth.py --collection no2 # tempo/no2/v04

under s3://airquality-data-store-develop/. Reads only snapshot metadata
and the native time axis, so ambient AWS credentials for the store bucket
suffice (no Earthdata credentials). A local store path works too. The
extracted history is cached next to the output GIF.
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import icechunk
import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import zarr
from matplotlib.animation import FFMpegWriter, FuncAnimation, PillowWriter
from tempo_collections import COLLECTIONS, DEFAULT_COLLECTION, STORE_URI

DATA_REGION = "us-west-2"  # same as build_s3_test_store.py
# The store's time axis unit, matching virtualizarr_processor.manifest.
TEMPO_EPOCH = datetime(1980, 1, 6, tzinfo=timezone.utc)

DEFAULT_FRAMES = 150
DEFAULT_FPS = 12

# Colorblind-safe categorical pair.
COLORS = {"append": "#2a78d6", "insert": "#eb6834"}
LABELS = {
    "append": "append at the axis end",
    "insert": "out-of-order insert (pending → re-sort)",
}


def save_animation(anim: FuncAnimation, gif_path: Path, fps: int) -> None:
    """Write the GIF plus an MP4 sibling (scrubbable; QuickTime-compatible).

    Uses the ffmpeg binary bundled with imageio-ffmpeg, so no system install
    is needed. Pause frames are literal duplicates, so they hold in both.
    """
    anim.save(gif_path, writer=PillowWriter(fps=fps))
    print(f"Wrote {gif_path} ({gif_path.stat().st_size / 1e6:.1f} MB)")
    import imageio_ffmpeg

    matplotlib.rcParams["animation.ffmpeg_path"] = imageio_ffmpeg.get_ffmpeg_exe()
    mp4_path = gif_path.with_suffix(".mp4")
    anim.save(
        mp4_path,
        writer=FFMpegWriter(
            fps=fps, codec="libx264", extra_args=["-pix_fmt", "yuv420p", "-crf", "20"]
        ),
    )
    print(f"Wrote {mp4_path} ({mp4_path.stat().st_size / 1e6:.1f} MB)")


def open_repo(store: str) -> icechunk.Repository:
    """Open the store read-only; native arrays need no virtual-chunk access."""
    if store.startswith("s3://"):
        bucket, _, prefix = store.removeprefix("s3://").partition("/")
        storage = icechunk.s3_storage(
            bucket=bucket, prefix=prefix.rstrip("/"), region=DATA_REGION, from_env=True
        )
    else:
        storage = icechunk.local_filesystem_storage(store)
    return icechunk.Repository.open(storage=storage)


def walk_history(repo: icechunk.Repository) -> dict[str, np.ndarray]:
    """Per-category (scan time, commit time) matplotlib date numbers.

    Diffs the time axis between consecutive ``main`` snapshots: a new value
    past the previous axis end is an append, anything else is an insert
    (the oldest retained snapshot's whole axis counts as appends).
    """
    snapshots = list(repo.ancestry(branch="main"))[::-1]  # oldest first
    out: dict[str, list[tuple[float, float]]] = {k: [] for k in COLORS}
    previous = np.array([])
    for n, snapshot in enumerate(snapshots, start=1):
        store = repo.readonly_session(snapshot_id=snapshot.id).store
        try:
            axis = np.asarray(zarr.open_array(store, path="time", mode="r")[:])
        except zarr.errors.ArrayNotFoundError:  # snapshots before the axis exists
            continue
        new = np.setdiff1d(axis, previous)
        if len(new):
            axis_end = previous.max() if previous.size else -np.inf
            committed = mdates.date2num(snapshot.written_at)
            for value in new:
                scan = mdates.date2num(TEMPO_EPOCH + timedelta(seconds=float(value)))
                out["append" if value > axis_end else "insert"].append(
                    (scan, committed)
                )
        previous = axis
        print(
            f"  {n}/{len(snapshots)} snapshots, {sum(map(len, out.values()))} granules",
            end="\r",
        )
    print()
    return {k: np.array(v).reshape(-1, 2) for k, v in out.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--collection", choices=sorted(COLLECTIONS), default=DEFAULT_COLLECTION
    )
    parser.add_argument(
        "--store-dir",
        help="s3:// or local store (default: the pipeline store for the collection)",
    )
    parser.add_argument("--frames", type=int, default=DEFAULT_FRAMES)
    parser.add_argument("--fps", type=int, default=DEFAULT_FPS)
    parser.add_argument("--dpi", type=int, default=90)
    parser.add_argument("--out", default=None, help="output GIF path")
    args = parser.parse_args()

    store = args.store_dir or STORE_URI.format(args.collection)
    out_path = Path(args.out or f"tempo-{args.collection}-store-growth.gif")

    print(f"Opening {store} read-only...")
    repo = open_repo(store)
    tip = next(iter(repo.ancestry(branch="main"))).id
    # Walking thousands of snapshots takes a while; cache per branch tip.
    cache = out_path.parent / f"history-{args.collection}-{tip[:12]}.npz"
    if cache.exists():
        print(f"Using cached history from {cache}")
        cats = {k: v for k, v in np.load(cache).items()}
    else:
        print("Walking snapshot ancestry...")
        cats = walk_history(repo)
        np.savez_compressed(cache, **cats)
    n_granules = sum(len(c) for c in cats.values())
    if not n_granules:
        sys.exit("no granules in the store's retained history")
    print(f"  {n_granules} granules over {args.frames} frames")

    committed_all = np.sort(np.concatenate([c[:, 1] for c in cats.values() if len(c)]))
    t0, t1 = committed_all[0], committed_all[-1]
    frame_times = np.linspace(t0, t1, args.frames)
    scan_all = np.concatenate([c[:, 0] for c in cats.values() if len(c)])

    fig, (ax, ax_len) = plt.subplots(
        2,
        1,
        figsize=(9.6, 6.4),
        dpi=args.dpi,
        height_ratios=[3, 1],
        sharex=False,
        constrained_layout=True,
    )
    fig.patch.set_facecolor("white")
    for a in (ax, ax_len):
        a.set_facecolor("white")
        a.grid(True, color="#eeedeb", linewidth=0.8)
        a.set_axisbelow(True)
        for spine in ("top", "right"):
            a.spines[spine].set_visible(False)

    collection_name = f"TEMPO_{args.collection.upper()}_L3 V04"
    ax.set_title(
        f"{collection_name} — granules arriving in the virtual Icechunk store",
        loc="left",
        fontsize=12,
    )
    ax.set_xlabel("scan time (the store's `time` axis)")
    ax.set_ylabel("commit time")
    scan_pad = (scan_all.max() - scan_all.min()) * 0.02
    ax.set_xlim(scan_all.min(), scan_all.max() + scan_pad)
    ax.set_ylim(t0, t1 + (t1 - t0) * 0.02)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.yaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))

    scatters = {
        kind: ax.scatter([], [], s=3, color=color, linewidths=0)
        for kind, color in COLORS.items()
    }
    ax.legend(
        handles=[
            plt.Line2D(
                [], [], linestyle="", marker="o", markersize=6, color=c, label=LABELS[k]
            )
            for k, c in COLORS.items()
        ],
        loc="upper left",
        frameon=True,
        framealpha=0.92,
        edgecolor="none",
        facecolor="white",
        fontsize=9,
    )
    status = ax.annotate(
        "",
        xy=(0.99, 0.02),
        xycoords="axes fraction",
        ha="right",
        fontsize=10,
        color="#52514e",
    )

    ax_len.set_ylabel("len(time)")
    ax_len.set_xlabel("commit time")
    ax_len.set_xlim(t0, t1)
    ax_len.set_ylim(0, n_granules * 1.05)
    ax_len.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    (len_line,) = ax_len.plot([], [], color=COLORS["append"], linewidth=2)
    len_label = ax_len.annotate(
        "",
        xy=(0, 0),
        xytext=(6, 0),
        textcoords="offset points",
        va="center",
        fontsize=9,
        color="#0b0b0b",
    )

    # side="right": a granule committed exactly at the frame time counts,
    # so the final frame holds the complete story.
    counts_upto = {
        kind: np.searchsorted(c[:, 1], frame_times, side="right")
        if len(c)
        else np.zeros(args.frames, dtype=int)
        for kind, c in cats.items()
    }

    # Ring + label shown while the animation holds on the first insert.
    highlight = ax.scatter(
        [], [], s=350, facecolors="none", edgecolors="#0b0b0b", linewidths=1.5, zorder=4
    )
    event_label = ax.annotate(
        "",
        xy=(0, 0),
        xytext=(-12, 14),
        textcoords="offset points",
        fontsize=10,
        fontweight="bold",
        color="#0b0b0b",
        ha="right",
        va="bottom",
    )

    def first_insert(frame: int) -> bool:
        counts = counts_upto["insert"]
        return counts[frame] > 0 and (frame == 0 or counts[frame - 1] == 0)

    def draw(frame: int):
        t = frame_times[frame]
        for kind, c in cats.items():
            n = counts_upto[kind][frame]
            scatters[kind].set_offsets(c[:n] if n else np.empty((0, 2)))
        total = int(sum(counts_upto[k][frame] for k in cats))
        inserts = int(counts_upto["insert"][frame])
        status.set_text(
            f"{mdates.num2date(t):%Y-%m-%d}   granules: {total:,}   "
            f"out-of-order: {inserts:,}"
        )
        n_committed = int(np.searchsorted(committed_all, t, side="right"))
        len_line.set_data(committed_all[:n_committed], np.arange(1, n_committed + 1))
        if n_committed:
            len_label.xy = (committed_all[n_committed - 1], n_committed)
            len_label.set_text(f"{n_committed:,}")
        if first_insert(frame):
            highlight.set_offsets(cats["insert"][:1])
            event_label.xy = tuple(cats["insert"][0])
            event_label.set_text("first out-of-order granule")
        else:
            highlight.set_offsets(np.empty((0, 2)))
            event_label.set_text("")
        return (*scatters.values(), status, len_line, len_label, highlight, event_label)

    # Hold for ~1 s on the frame where the first out-of-order granule shows up.
    frame_seq: list[int] = []
    for frame in range(args.frames):
        frame_seq.append(frame)
        if first_insert(frame):
            frame_seq.extend([frame] * args.fps)

    anim = FuncAnimation(fig, draw, frames=frame_seq, blit=True)
    print(f"Rendering {len(frame_seq)} frames to {out_path}...")
    save_animation(anim, out_path, args.fps)
    return 0


if __name__ == "__main__":
    sys.exit(main())
