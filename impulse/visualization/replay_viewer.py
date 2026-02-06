"""
Replay Viewer - Interactive Jupyter visualization for replay data.

Provides a 3D animation of ball and player positions with synchronized
2D feature plots and playback controls.

Usage:
    from impulse import ReplayDataset
    from impulse.visualization import ReplayViewer

    dataset = ReplayDataset(db_path='./impulse.db', data_dir='./parsed')
    replay = dataset.load_replay('some-replay-id')

    viewer = ReplayViewer(replay)
    viewer.display()
"""

import threading
from typing import List, Dict, Any, Optional, Tuple
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from mpl_toolkits.mplot3d.art3d import Line3DCollection
import ipywidgets as widgets

from impulse.replay_dataset import ReplayData


# Color palettes
BLUE_TEAM_COLORS = ['#3b82f6', '#8b5cf6', '#22c55e', '#06b6d4']  # blue, purple, green, cyan
ORANGE_TEAM_COLORS = ['#f97316', '#ef4444', '#ec4899', '#eab308']  # orange, red, pink, yellow
BALL_COLOR = '#d1d5db'  # light gray/silver


def quaternion_to_forward(qx: float, qy: float, qz: float, qw: float) -> np.ndarray:
    """
    Convert quaternion to forward direction vector.

    Rocket League coordinate system:
    - X: right/left
    - Y: forward/back
    - Z: up/down

    Args:
        qx, qy, qz, qw: Quaternion components (scalar-last format)

    Returns:
        np.ndarray of shape (3,) - unit forward vector
    """
    from scipy.spatial.transform import Rotation
    r = Rotation.from_quat([qx, qy, qz, qw])
    return r.apply([0, 1, 0])  # RL forward axis


def get_player_color(player_idx: int, team: int) -> str:
    """Get color for a player based on their index within their team."""
    if team == 0:
        return BLUE_TEAM_COLORS[player_idx % len(BLUE_TEAM_COLORS)]
    else:
        return ORANGE_TEAM_COLORS[player_idx % len(ORANGE_TEAM_COLORS)]


class ReplayViewer:
    """
    Interactive Jupyter widget for visualizing Rocket League replay data.

    Features:
    - 3D view with ball and car positions (spheres/arrows)
    - Player labels showing names from metadata
    - Play/pause/scrub controls for frame navigation
    - Synchronized 2D feature plots with moving frame marker

    Usage:
        viewer = ReplayViewer(replay)
        viewer.display()

        # With customization
        viewer = ReplayViewer(
            replay,
            features=['Ball - position z', 'p0_boost level'],
            start_frame=1400,
            end_frame=1600,
            playback_fps=15.0
        )
        viewer.display()
    """

    def __init__(
        self,
        replay: ReplayData,
        features: Optional[List[str]] = None,
        start_frame: int = 0,
        end_frame: Optional[int] = None,
        playback_fps: float = 10.0,
        figsize_3d: Tuple[float, float] = (5, 5),
        figsize_2d: Tuple[float, float] = (5, 4),
    ):
        """
        Initialize the replay viewer.

        Args:
            replay: ReplayData object to visualize
            features: List of column names to plot in 2D time series.
                     Defaults to ['Ball - position z'] if None.
            start_frame: First frame to display (inclusive)
            end_frame: Last frame to display (exclusive). Defaults to all frames.
            playback_fps: Frames per second for playback animation
            figsize_3d: Size of 3D visualization figure
            figsize_2d: Size of each 2D time series figure
        """
        self.replay = replay
        self.frames = replay.frames
        self.metadata = replay.metadata

        # Get number of players from metadata
        self.num_players = self._get_num_players()

        # Frame range
        self.start_frame = start_frame
        self.end_frame = end_frame if end_frame is not None else len(self.frames)
        self.current_frame = start_frame

        # Features to plot
        self.features = features if features is not None else ['Ball - position z']

        # Playback settings
        self.playback_fps = playback_fps
        self.speed_multiplier = 1.0
        self.is_playing = False
        self._timer = None

        # Figure sizes
        self.figsize_3d = figsize_3d
        self.figsize_2d = figsize_2d

        # Player info from metadata
        self._setup_player_info()

        # Will be set up during display()
        self.fig_3d = None
        self.ax_3d = None
        self.fig_2d = None
        self.axes_2d = None

        # Artists (plot elements to update)
        self.ball_scatter = None
        self.player_scatters = {}
        self.player_arrows = {}
        self.player_labels = {}
        self.frame_markers = []

    def _get_num_players(self) -> int:
        """Determine the number of players from metadata."""
        # Try parsing_info first
        parsing_info = self.metadata.get('parsing_info', {})
        if 'num_players' in parsing_info:
            return parsing_info['num_players']

        # Fallback: count non-None entries in player_mapping
        player_mapping = self.metadata.get('player_mapping', {})
        count = sum(1 for v in player_mapping.values() if v is not None)
        if count > 0:
            return count

        # Last resort: derive from team_size
        team_size = self.metadata.get('team_size', 3)
        return team_size * 2

    def _setup_player_info(self):
        """Extract player info from metadata."""
        self.player_info = {}
        player_mapping = self.metadata.get('player_mapping', {})

        # Count players per team for color assignment
        team_counts = {0: 0, 1: 0}

        for idx in range(self.num_players):
            idx_str = str(idx)
            info = player_mapping.get(idx_str)

            if info is None:
                # Player slot exists but no metadata
                self.player_info[idx] = {
                    'name': f'Player {idx}',
                    'team': 0 if idx < self.num_players // 2 else 1,
                    'color': '#888888',
                }
                continue

            team = info.get('team', 0)
            name = info.get('name', f'Player {idx}')

            color = get_player_color(team_counts[team], team)
            team_counts[team] += 1

            self.player_info[idx] = {
                'name': name,
                'team': team,
                'color': color,
            }

    def _get_entity_positions(self, frame_idx: int) -> Dict[str, Optional[np.ndarray]]:
        """Extract ball and player positions for a single frame."""
        row = self.frames.iloc[frame_idx]
        positions = {}

        # Ball position
        ball_x = row.get('Ball - position x')
        ball_y = row.get('Ball - position y')
        ball_z = row.get('Ball - position z')
        if pd.notna(ball_x) and pd.notna(ball_y) and pd.notna(ball_z):
            positions['ball'] = np.array([ball_x, ball_y, ball_z])
        else:
            positions['ball'] = None

        # Player positions
        for i in range(self.num_players):
            px = row.get(f'p{i}_position x')
            py = row.get(f'p{i}_position y')
            pz = row.get(f'p{i}_position z')
            if pd.notna(px) and pd.notna(py) and pd.notna(pz):
                positions[f'p{i}'] = np.array([px, py, pz])
            else:
                positions[f'p{i}'] = None

        return positions

    def _get_entity_orientations(self, frame_idx: int) -> Dict[str, Optional[np.ndarray]]:
        """Extract player orientations (as forward vectors) for a single frame."""
        row = self.frames.iloc[frame_idx]
        orientations = {}

        for i in range(self.num_players):
            qx = row.get(f'p{i}_quaternion x')
            qy = row.get(f'p{i}_quaternion y')
            qz = row.get(f'p{i}_quaternion z')
            qw = row.get(f'p{i}_quaternion w')

            if all(pd.notna(q) for q in [qx, qy, qz, qw]):
                orientations[f'p{i}'] = quaternion_to_forward(qx, qy, qz, qw)
            else:
                orientations[f'p{i}'] = None

        return orientations

    def _setup_3d_figure(self):
        """Create 3D figure with initial artists."""
        self.fig_3d, self.ax_3d = plt.subplots(
            subplot_kw={'projection': '3d'},
            figsize=self.figsize_3d
        )

        # Set axis labels
        self.ax_3d.set_xlabel('X')
        self.ax_3d.set_ylabel('Y')
        self.ax_3d.set_zlabel('Z')
        self.ax_3d.set_title('Replay Viewer')

        # Initial positions
        positions = self._get_entity_positions(self.current_frame)
        orientations = self._get_entity_orientations(self.current_frame)

        # Ball scatter
        ball_pos = positions.get('ball')
        if ball_pos is not None:
            self.ball_scatter = self.ax_3d.scatter(
                [ball_pos[0]], [ball_pos[1]], [ball_pos[2]],
                c=BALL_COLOR, s=100, marker='o', label='Ball'
            )
        else:
            self.ball_scatter = self.ax_3d.scatter(
                [], [], [], c=BALL_COLOR, s=100, marker='o', label='Ball'
            )

        # Player scatters and arrows
        for i in range(self.num_players):
            key = f'p{i}'
            pos = positions.get(key)
            fwd = orientations.get(key)

            # Get player info
            info = self.player_info.get(i, {'name': f'P{i}', 'color': '#888888'})
            color = info['color']
            name = info['name']

            if pos is not None:
                # Player dot
                self.player_scatters[key] = self.ax_3d.scatter(
                    [pos[0]], [pos[1]], [pos[2]],
                    c=color, s=80, marker='o'
                )

                # Player label
                self.player_labels[key] = self.ax_3d.text(
                    pos[0], pos[1], pos[2] + 150,
                    name, fontsize=8, ha='center', color=color
                )

                # Orientation arrow
                if fwd is not None:
                    arrow_scale = 200
                    self.player_arrows[key] = self.ax_3d.quiver(
                        pos[0], pos[1], pos[2],
                        fwd[0] * arrow_scale, fwd[1] * arrow_scale, fwd[2] * arrow_scale,
                        color=color, arrow_length_ratio=0.3
                    )
                else:
                    self.player_arrows[key] = None
            else:
                # Invisible placeholders
                self.player_scatters[key] = self.ax_3d.scatter(
                    [], [], [], c=color, s=80, marker='o'
                )
                self.player_labels[key] = self.ax_3d.text(
                    0, 0, 0, name, fontsize=8, ha='center', color=color, visible=False
                )
                self.player_arrows[key] = None

        # Set reasonable axis limits based on Rocket League field size
        # RL field is roughly -4096 to 4096 in X, -5120 to 5120 in Y, 0 to 2044 in Z
        self.ax_3d.set_xlim(-5000, 5000)
        self.ax_3d.set_ylim(-6000, 6000)
        self.ax_3d.set_zlim(0, 2500)

    def _setup_2d_figure(self):
        """Create 2D time series figure with feature plots."""
        n_features = len(self.features)
        self.fig_2d, self.axes_2d = plt.subplots(
            n_features, 1,
            figsize=(self.figsize_2d[0], self.figsize_2d[1] * n_features),
            sharex=True
        )

        # Handle single feature case
        if n_features == 1:
            self.axes_2d = [self.axes_2d]

        # Get frame range data
        frame_data = self.frames.iloc[self.start_frame:self.end_frame]

        # Always use frame numbers for x-axis
        x_values = np.arange(self.start_frame, self.end_frame)
        self.x_values = x_values

        for i, (ax, feature) in enumerate(zip(self.axes_2d, self.features)):
            if feature in frame_data.columns:
                y_values = frame_data[feature].values
                ax.plot(x_values, y_values, linewidth=1)

            ax.set_ylabel(feature, fontsize=8)
            ax.tick_params(labelsize=8)

            # Add vertical line marker for current frame
            marker = ax.axvline(x=self.current_frame, color='red', linewidth=1.5, alpha=0.7)
            self.frame_markers.append(marker)

        self.axes_2d[-1].set_xlabel('Frame', fontsize=9)

    def _setup_controls(self):
        """Create playback control widgets."""
        # Play/Pause button
        self.play_button = widgets.Button(
            description='Play',
            icon='play',
            layout=widgets.Layout(width='80px')
        )
        self.play_button.on_click(self._on_play_pause)

        # Frame slider
        self.frame_slider = widgets.IntSlider(
            value=self.current_frame,
            min=self.start_frame,
            max=self.end_frame - 1,
            description='Frame:',
            continuous_update=False,
            layout=widgets.Layout(width='400px')
        )
        self.frame_slider.observe(self._on_slider_change, names='value')

        # Speed slider
        self.speed_slider = widgets.FloatSlider(
            value=1.0,
            min=0.25,
            max=4.0,
            step=0.25,
            description='Speed:',
            continuous_update=True,
            layout=widgets.Layout(width='200px')
        )
        self.speed_slider.observe(self._on_speed_change, names='value')

        # Frame label
        self.frame_label = widgets.Label(
            value=self._format_frame_label()
        )

        # Arrange controls
        self.controls = widgets.HBox([
            self.play_button,
            self.frame_slider,
            self.speed_slider,
            self.frame_label
        ])

    def _format_frame_label(self) -> str:
        """Format the frame counter label."""
        total = self.end_frame - self.start_frame
        current = self.current_frame - self.start_frame
        return f'{current} / {total}'

    def _update_3d_view(self):
        """Update 3D view for current frame."""
        positions = self._get_entity_positions(self.current_frame)
        orientations = self._get_entity_orientations(self.current_frame)

        # Update ball
        ball_pos = positions.get('ball')
        if ball_pos is not None:
            self.ball_scatter._offsets3d = ([ball_pos[0]], [ball_pos[1]], [ball_pos[2]])

        # Update players
        for i in range(self.num_players):
            key = f'p{i}'
            pos = positions.get(key)
            fwd = orientations.get(key)

            if pos is not None:
                # Update position
                self.player_scatters[key]._offsets3d = ([pos[0]], [pos[1]], [pos[2]])
                self.player_scatters[key].set_visible(True)

                # Update label position
                self.player_labels[key].set_position((pos[0], pos[1]))
                self.player_labels[key].set_3d_properties(pos[2] + 150)
                self.player_labels[key].set_visible(True)

                # Update orientation arrow - need to remove old and create new
                if self.player_arrows.get(key) is not None:
                    self.player_arrows[key].remove()

                if fwd is not None:
                    info = self.player_info.get(i, {'color': '#888888'})
                    arrow_scale = 200
                    self.player_arrows[key] = self.ax_3d.quiver(
                        pos[0], pos[1], pos[2],
                        fwd[0] * arrow_scale, fwd[1] * arrow_scale, fwd[2] * arrow_scale,
                        color=info['color'], arrow_length_ratio=0.3
                    )
                else:
                    self.player_arrows[key] = None
            else:
                self.player_scatters[key].set_visible(False)
                self.player_labels[key].set_visible(False)
                if self.player_arrows.get(key) is not None:
                    self.player_arrows[key].remove()
                    self.player_arrows[key] = None

        self.fig_3d.canvas.draw_idle()

    def _update_2d_markers(self):
        """Update frame markers on 2D plots."""
        if not self.frame_markers:
            return

        idx = self.current_frame - self.start_frame
        if 0 <= idx < len(self.x_values):
            current_x = self.x_values[idx]
            for marker in self.frame_markers:
                marker.set_xdata([current_x, current_x])

        self.fig_2d.canvas.draw_idle()

    def _render_frame(self):
        """Update all views for current frame."""
        self._update_3d_view()
        self._update_2d_markers()
        self.frame_label.value = self._format_frame_label()

    def _on_play_pause(self, button):
        """Toggle playback state."""
        if self.is_playing:
            self.pause()
            button.description = 'Play'
            button.icon = 'play'
        else:
            self.play()
            button.description = 'Pause'
            button.icon = 'pause'

    def _on_slider_change(self, change):
        """Handle frame slider value change."""
        self.current_frame = change['new']
        self._render_frame()

    def _on_speed_change(self, change):
        """Handle speed slider value change."""
        self.speed_multiplier = change['new']

    def _animation_step(self):
        """Single animation step - called repeatedly during playback."""
        if not self.is_playing:
            return

        # Advance frame
        next_frame = self.current_frame + 1
        if next_frame >= self.end_frame:
            self.pause()
            self.play_button.description = 'Play'
            self.play_button.icon = 'play'
            return

        # Update slider (triggers _on_slider_change -> _render_frame)
        self.frame_slider.value = next_frame

        # Schedule next step
        delay = 1.0 / (self.playback_fps * self.speed_multiplier)
        self._timer = threading.Timer(delay, self._animation_step)
        self._timer.start()

    def play(self):
        """Start playback."""
        self.is_playing = True
        self._animation_step()

    def pause(self):
        """Pause playback."""
        self.is_playing = False
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def goto_frame(self, frame_idx: int):
        """Jump to a specific frame."""
        if self.start_frame <= frame_idx < self.end_frame:
            self.frame_slider.value = frame_idx

    def display(self) -> widgets.VBox:
        """
        Display the viewer in a Jupyter notebook.

        Returns:
            widgets.VBox: The widget container (auto-displayed by Jupyter)
        """
        # Disable auto-display while creating figures
        plt.ioff()

        # Setup figures (ipympl makes fig.canvas a widget)
        self._setup_3d_figure()
        self._setup_2d_figure()

        # Re-enable interactive mode
        plt.ion()

        # Setup controls
        self._setup_controls()

        # Create layout with plots side by side using figure canvases directly
        plots_row = widgets.HBox([
            self.fig_3d.canvas,
            self.fig_2d.canvas
        ])

        layout = widgets.VBox([
            self.controls,
            plots_row
        ])

        return layout

    def __del__(self):
        """Cleanup timer on deletion."""
        self.pause()
