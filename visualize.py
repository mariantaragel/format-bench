##
# @file visualize.py
# @author Marián Tarageľ (xtarag01)
# @brief Display results of the benchmarks

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import pandas as pd

def show_results(location: str, report_path: str = None):
    """
    Print results to the terminal, write them to CSV file and optionally create report
    
    location : name of created CSV file with results
    report_path : name of created report
    """
    print(f"· Writing results to {location}")
    results.to_csv(location, index=False)

    if report_path:
        print(f"· Creating report {report_path}")
        make_report(results, report_path)

    print()
    print(results)
    print()

def make_report(df : pd.DataFrame, save_path: str, show_figure: bool = False):
    """
    Create report from the benchmark results

    save_path : the location where the report will be saved 
    show_figure : whether to display the report or not
    """
    df = df[["format_name","save_time (s)", "read_time (s)", "save_peak_mem (MB)", "read_peak_mem (MB)", "file_size (MB)"]]

    fig = plt.figure(figsize=(8.27, 11.69), layout="tight")
    ax1 = plt.subplot2grid((3, 4), (0, 0), colspan=2)
    ax2 = plt.subplot2grid((3, 4), (0, 2), colspan=2, sharey=ax1)
    ax3 = plt.subplot2grid((3, 4), (1, 0), colspan=2)
    ax4 = plt.subplot2grid((3, 4), (1, 2), colspan=2, sharey=ax3)
    ax5 = plt.subplot2grid((3, 4), (2, 1), colspan=2)
    axes = [ax1, ax2, ax3, ax4, ax5]
    columns = df.columns.to_list()

    green = "#55a868"
    red = "#c44e52"

    for i, ax in enumerate(axes):
        color_labels = [green if x < df[columns[i + 1]].mean() else red for x in df[columns[i + 1]]]
        rect = ax.bar(df["format_name"], df[columns[i + 1]], color=color_labels)
        ax.bar_label(rect, padding=2, fmt="%.2f")
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis='y')
        ax.set_axisbelow(True)

    axes[0].set_title("Save Time")
    axes[1].set_title("Read Time")
    axes[2].set_title("Save Peak Memory")
    axes[3].set_title("Read Peak Memory")
    axes[4].set_title("Total Size")

    for i in range(2):
        axes[i].set_ylabel("Time (s)")

    for i in range(2, 4):
        axes[i].set_ylabel("Peak Memory (MB)")

    axes[4].set_ylabel("Total Size (MB)")

    fig.suptitle("Benchmark report")

    red_patch = mpatches.Patch(color=red, label="Above Average")
    green_patch = mpatches.Patch(color=green, label="Below Average")
    fig.legend(handles=[red_patch, green_patch], loc="upper left")

    if save_path:
        fig.savefig(save_path, format="pdf", bbox_inches="tight")

    if show_figure:
        plt.show()

    plt.close(fig)