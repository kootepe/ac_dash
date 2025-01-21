from .tools.influxdb_funcs import init_client, just_read, read_ifdb
from plotly.graph_objs import Scattergl
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import logging

from  .data_mgt import flux_table_to_df, flux_to_df

logger = logging.getLogger("defaultLogger")


def mk_attribute_plot(
    measurement,
    df,
    selected_chambers,
    index,
    date_range,
    attribute,
    gas=None,
):
    current_measurement = measurement

    if gas is None:
        attribute_value = getattr(current_measurement, attribute)
        attribute_name = attribute
    else:
        attribute_value = getattr(current_measurement, attribute).get(gas)
        attribute_name = f"{gas}_{attribute}"
    logger.debug(f"Creating {attribute_name} graph.")

    if df is None:
        return go.Figure()
    df["datetime"] = df["start_time"]
    df.set_index("datetime", inplace=True)

    df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)
    color_map = create_color_mapping(df, "chamber_id")

    # Create a list of traces, one for each unique `id`
    traces = []
    symbol_map = {
        False: "x-thin",
        True: "circle",
    }
    for unique_id, color in color_map.items():
        filtered_df = df[df["chamber_id"] == unique_id]
        marker_symbols = filtered_df["is_valid"].map(symbol_map).fillna("x-thin")
        traces.append(
            go.Scattergl(
                x=filtered_df.index,
                y=filtered_df[attribute_name],
                mode="markers",
                name=f"{unique_id}",
                marker=dict(
                    color=color,
                    symbol=marker_symbols,
                    size=4,
                    line=dict(color=color, width=1.5),
                ),
                hoverinfo="all",
            )
        )

    # Add the highlighter trace
    highlighter = apply_highlighter(current_measurement, attribute, gas)

    # settings for the highlighter
    layout = go.Layout(
        hovermode="closest",
        hoverdistance=30,
        title={"text": attribute_name},
        margin=dict(l=10, r=10, t=30, b=10),
        xaxis=dict(
            type="date",
            showspikes=False,
            spikethickness=1,
            spikedash="solid",
        ),
        yaxis=dict(
            showspikes=False,
            spikethickness=1,
            spikedash="solid",
        ),
        legend=dict(
            font=dict(size=13),
            orientation="h",
            tracegroupgap=3,
            # itemclick=False,
            # itemdoubleclick=False,
        ),
    )

    # Add all traces (separate traces for each id) and the highlighter trace to the figure
    fig = go.Figure(data=traces + [highlighter], layout=layout)

    yrange = df[attribute_name].max() - df[attribute_name].min()
    yrange_perc = yrange * 0.1
    fig.update_yaxes(
        autorangeoptions_maxallowed=df[attribute_name].max() + yrange_perc,
        autorangeoptions_minallowed=df[attribute_name].min() - yrange_perc,
        scaleanchor=False,
    )
    # add line at 0 lagtime
    fig.add_hline(y=0, line_dash="dash", line_color="blue", line_width=1)

    return fig


fixed_color_mapping = {}
# drop the orange from the second list
color_list = px.colors.qualitative.Plotly + px.colors.qualitative.D3[2:]


def create_color_mapping(df, column_name):
    """Create a deterministic color map where the same value always gets the same color."""
    unique_values = sorted(df[column_name].unique())
    num_colors = len(color_list)
    logger.debug(color_list)

    # Create a mapping using a hash of the value to select a color
    color_mapping = {val: color_list[hash(val) % num_colors] for val in unique_values}

    return color_mapping


def apply_highlighter(current_measurement, attribute, gas=None):
    """Create a highlighter for the lag graph."""
    # NOTE: Is this query needed when a value gets updated by clicking on of the
    # buttons in the main app? Seems irrelevant.
    pt = flux_to_df(
        current_measurement.start_time, current_measurement.instrument.serial
    )
    if gas is not None:
        df_attr = f"{gas}_{attribute}"
    else:
        df_attr = attribute
    val = None
    if pt is None or pt.empty:
        pass
    else:
        val = pt.loc[0, df_attr]
        logger.debug(f"val: {val}")

    if gas is None:
        logger.debug("Gas is None")
        logger.debug(getattr(current_measurement, attribute))
        attr_value = getattr(current_measurement, attribute)
    else:
        logger.debug("Gas is not None")
        logger.debug(getattr(current_measurement, attribute))
        attr_value = getattr(current_measurement, attribute).get(gas)

    logger.debug(attribute)
    logger.debug(gas)
    logger.debug(attr_value)

    highlighter = Scattergl(
        x=[current_measurement.start_time],
        # y=[attr_value],
        y=[val] if val is not None else [attr_value],
        mode="markers",
        marker=dict(
            symbol="circle",
            size=15,
            color="rgba(255,0,0,0)",
            line=dict(color="rgba(255,0,0,1)", width=2),
        ),
        name="Current",
        hoverinfo="none",
        showlegend=True,
    )
    return highlighter


def apply_graph_zoom(
    figure,
    triggered_id,
    relayout_data,
):
    """Apply zoom to the lag graph if zoom state is available."""
    logger.debug(relayout_data)
    # logger.debug(figure.layout)
    if relayout_data:
        figure.update_layout(graph_zoom(relayout_data, triggered_id))


def graph_zoom(relayout, triggered_id):
    """Handle zooming logic."""
    # BUG: Moving shapes in the figure causes the zoom to reset. Fix by using
    # dash stores?
    # BUG: Zooming with just free x or y axis to the gas graph after zooming
    # with free x/y axis doesnt work properly
    logger.debug(triggered_id)
    xaxis = None
    yaxis = None
    if relayout and "xaxis.range[0]" in relayout:
        xaxis = {
            "xaxis": {
                # sometimes when zooming out with double-click autorange True gets
                # added to the layout
                "autorange": False,
                "range": [
                    relayout["xaxis.range[0]"],
                    relayout["xaxis.range[1]"],
                ],
            }
        }
    if relayout and "yaxis.range[0]" in relayout:
        yaxis = {
            "yaxis": {
                "autorange": False,
                "range": [
                    relayout["yaxis.range[0]"],
                    relayout["yaxis.range[1]"],
                ],
            }
        }
    # when attribute plots are zoomed, then doubleclicked to reset zoom and then
    # selecting a new point, the plot resets to the zoomed state which is
    # incorrect. This fixes it.
    if relayout and "xaxis.autorange" in relayout:
        xaxis = {"xaxis": {"autorange": True, "range": None}}
    if relayout and "yaxis.autorange" in relayout:
        yaxis = {"yaxis": {"autorange": True, "range": None}}

    if xaxis is not None and yaxis is not None:
        new_layout = {**xaxis, **yaxis}
        logger.debug(f"Applying: {new_layout}")
        return new_layout
    if xaxis is not None:
        new_layout = {**xaxis}
        logger.debug(f"Applying: {new_layout}")
        return new_layout
    if yaxis is not None:
        new_layout = {**yaxis}
        logger.debug(f"Applying: {new_layout}")
        return new_layout
    return None


def check_skips(df, skip_valid, skip_invalid):
    logger.debug(f"This is skip_invalid {skip_invalid}")
    # empty list and None both evaluate to False
    if not skip_invalid:
        pass
    else:
        m = df["is_valid"] == True
        df = df[m]

    logger.debug(f"This is skip_valid {skip_valid}")
    # empty list and None both evaluate to False
    if not skip_valid:
        pass
    else:
        m = df["is_valid"] == False
        df = df[m]

    return df
