import osmnx as ox
import geopandas as gpd
from shapely.errors import TopologicalError

def precompute_and_save_data(north, south, east, west, road_path, industrial_path):
    print("Starting data pre-computation...\n")
    bbox = (north, south, east, west)

    # 1️⃣ Download and save road network data
    try:
        print(f"Downloading road network for bbox: {bbox}...")
        graph = ox.graph_from_bbox(bbox=bbox, network_type='all')  # ✅ Correct for OSMnx ≥ 2.0
        roads_gdf = ox.graph_to_gdfs(graph, nodes=False, edges=True)
        roads_gdf = roads_gdf[['geometry', 'highway', 'length']]
        roads_gdf.to_file(road_path, driver='GPKG', layer='roads')
        print(f"✅ Successfully saved road data to {road_path}")
    except Exception as e:
        print(f"❌ Could not download or save road data. Error: {e}")

    # 2️⃣ Download, clean, and save industrial area data
    try:
        print(f"Downloading industrial landuse for bbox: {bbox}...")
        tags = {"landuse": "industrial"}
        industrial_gdf = ox.features_from_bbox(bbox=bbox, tags=tags)  # ✅ Correct for OSMnx ≥ 2.0

        if industrial_gdf.empty:
            print("⚠️ No industrial features found in the specified bounding box.")
            return

        print("Cleaning downloaded industrial data...")

        # Drop invalid and empty geometries
        industrial_gdf = industrial_gdf[industrial_gdf['geometry'].notna()]
        industrial_gdf = industrial_gdf[industrial_gdf.geom_type.isin(['Polygon', 'MultiPolygon'])]

        fixed_geoms = []
        for geom in industrial_gdf['geometry']:
            try:
                if geom.is_valid:
                    fixed_geoms.append(geom)
                else:
                    fixed_geoms.append(geom.buffer(0))  # Repair geometry
            except TopologicalError:
                fixed_geoms.append(None)

        industrial_gdf['geometry'] = fixed_geoms
        industrial_gdf = industrial_gdf[
            industrial_gdf['geometry'].notna() &
            industrial_gdf.is_valid &
            ~industrial_gdf['geometry'].is_empty
        ]

        if industrial_gdf.empty:
            print("⚠️ No valid industrial geometries remain after cleaning.")
            return
        
        clean_gdf = gpd.GeoDataFrame({
            'geometry': industrial_gdf['geometry'],
            'landuse': 'industrial'
        }, crs=industrial_gdf.crs)

        clean_gdf.to_file(industrial_path, driver='GPKG', layer='industrial')
        print(f"✅ Successfully cleaned and saved industrial data to {industrial_path}")

    except Exception as e:
        print(f"❌ An error occurred during the industrial data processing. Error: {e}")

# ============================================
# Example usage
# ============================================
if __name__ == '__main__':
    NORTH, SOUTH, EAST, WEST = 55, 19, -46, -136
    ROADS_FILE = "./MODEL/geolocation/precomputed_roads.gpkg"
    INDUSTRIAL_FILE = "./MODEL/geolocation/precomputed_industrial.gpkg"
    precompute_and_save_data(NORTH, SOUTH, EAST, WEST, ROADS_FILE, INDUSTRIAL_FILE)
    print("\nPre-computation complete.")
