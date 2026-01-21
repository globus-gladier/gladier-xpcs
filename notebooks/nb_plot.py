import h5py
import matplotlib.pyplot as plt


def start_new_plot(title="1D Scattering Curves", logx=False, logy=False):
    """Start a new figure for overlaying scattering data plots with optional log scaling."""
    plt.figure(figsize=(8, 5))
    ax = plt.gca()

    if logx:
        ax.set_xscale('log')
    if logy:
        ax.set_yscale('log')

    plt.xlabel("q (1/Å)", fontsize=12)
    plt.ylabel("Intensity (a.u.)", fontsize=12)
    plt.title(title, fontsize=14)
    plt.grid(True, which='both', linestyle='--', alpha=0.6)

def plot_hdf_overlay(file: str, 
                     x: str = '/xpcs/qmap/static_v_list_dim0', 
                     y: str = '/xpcs/temporal_mean/scattering_1d', 
                     label: str = None):
    """Add a curve from an HDF5 file to the current plot."""
    with h5py.File(file, 'r') as f:
        x_set = f[x][()]
        y_set = f[y][()]

        # Flatten and align lengths
        x_set = x_set.flatten()
        y_set = y_set.flatten()
        min_len = min(len(x_set), len(y_set))
        x_set = x_set[:min_len]
        y_set = y_set[:min_len]

        # Default label
        if label is None:
            label = file.split('/')[-1]

        # Add to current figure
        plt.plot(x_set, y_set, marker='o', linestyle='-', linewidth=2, markersize=4, label=label)

def show_plot():
    """Finalize and display the plot."""
    plt.legend()
    plt.tight_layout()
    plt.show()
    return plt.gcf()

