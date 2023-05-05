import geopandas as gpd
import osmnx as ox
import numpy as np
import pandas as pd

print("### LOAD DATA ###")

veget = gpd.read_file("./input_data/veget_strat.gpkg")
network_buffer = gpd.read_file("./input_data/network_metrop_buffer.gpkg")

network_buffer.to_crs(4171)

network_path = "./input_data/network_metrop.gpkg"

# network_edges = gpd.read_file("./input_data/network_metrop.gpkg" layer="edges")
# network_nodes = gpd.read_file("./input_data/network_metrop.gpkg", layer="nodes")

print("### CLIP VEGET ###")

clipped_veget = gpd.clip(veget, network_buffer)

clipped_veget_path = "./output_data/clipped_veget.gpkg"
clipped_veget.to_file(clipped_veget_path, driver="GPKG")

print("### JOIN VEGET NETWORK ###")


### FUNCTION TO CALCULATE IF FOR EACH DATA

def calculate_IF(input_path, output_path, fn, name):
    """Function to recalculate IF according to other attributes
    fn is the function to apply
    """
    data = gpd.read_file(input_path)

    data[f"IF_{name}"] = data.apply(fn, axis=1)

    data.to_file(output_path, driver="GPKG")

def veget_IF(row):
    """Return value of IF for temperature data"""
    if(row["vegetation_class"] == 1):
        return 0.75
    elif(row["vegetation_class"] == 2):
        return 0.75
    elif(row["vegetation_class"] == 3):
        return 0.5
    elif(row["vegetation_class"] == 4):
        return 0.01
    elif(row["vegetation_class"] == 5):
        return 0.01
    else:
        return 1
    
veget_IF_path = "./output_data/veget_IF.gpkg"
calculate_IF(clipped_veget_path, veget_IF_path, veget_IF, "veget")

def network_weighted_average(default_network, weighted_edges, layer_name, output_path):
    """This function calculate the weighted average for one attribute of one edge
    for the OSM network.

    For example, if for one segment we have 3 kind of vegetation, we want to calculate 
    the weighted average of the vegetation for this segment 
    """

    default_edges = gpd.read_file(default_network, layer="edges")

    default_nodes = gpd.read_file(default_network, layer="nodes")

    # For some reason pandas convert u, v and key into float for weighted_edges
    weighted_edges[["u", "v", "key"]] = weighted_edges[["u", "v", "key"]].astype(int)
    weighted_edges = weighted_edges.drop_duplicates(subset=["u", "v", "key"])
    weighted_edges = weighted_edges.set_index(["u", "v", "key"])

    print(f"Calculating weighted average for {layer_name} ...")

    # Due to intersection, there more features into the weighted_edges dataframe than the default_network one
    #The following line allows to recalculate the weighted average for one edge taking account all the "subedges"
    grouped_edges = weighted_edges.groupby(["u", "v", "key"], group_keys=True).apply(lambda x: pd.Series({
        f"IF_{layer_name}": np.average(x[f"IF_{layer_name}"], weights=x["cal_length"])
    })).reset_index()

    grouped_edges = grouped_edges.set_index(["u", "v", "key"])
    default_edges = default_edges.set_index(["u", "v", "key"])

    default_edges[f"IF_{layer_name}"] = grouped_edges[f"IF_{layer_name}"]

    default_nodes = default_nodes.set_index(['osmid'])

    G = ox.graph_from_gdfs(default_nodes, default_edges)

    print(f"Done. \nSaving file into {output_path}")

    ox.save_graph_geopackage(G, filepath=output_path)

def join_network_layer(network_path, layer_path, layer_name, output_path):
    """This function join a network with a specific layer"""
    network_edges = gpd.read_file(network_path, layer="edges")

    layer = gpd.read_file(layer_path)

    # if(layer_name == "temp_surface_road_raw"):
    #     layer = layer.to_crs(3946)
    # else:
    layer = layer.to_crs(network_edges.crs)

    print(f"Joining {layer_name} with osm network")

    joined_edges = gpd.overlay(network_edges, layer, how="intersection", keep_geom_type=True)

    # Convert into geoserie in order to calculate the length of the intersection

    joined_edges_serie = gpd.GeoSeries(joined_edges["geometry"])

    joined_edges_serie = joined_edges_serie.to_crs(32631)

    joined_edges["cal_length"] = joined_edges_serie.length

    network_weighted_average(network_path, joined_edges, layer_name, output_path)

network_veget_path = "./output_data/network_veget_weighted.gpkg"

join_network_layer(network_path, veget_IF_path, "veget", network_veget_path)




