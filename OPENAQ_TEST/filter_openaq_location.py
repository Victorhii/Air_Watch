import re

def filter_locations(input_file, output_file):
    tempo_min_lat = 15.0
    tempo_max_lat = 63.0
    tempo_min_lon = -148.8
    tempo_max_lon = -29.3478

    with open(input_file, 'r', encoding='utf-8', errors='ignore') as infile, \
        open(output_file, 'w', encoding='utf-8') as outfile:

        for line in infile:
            # Updated regex to match the actual format: 'ID | Name | Country | Lat,Lon'
            match = re.search(r"\|\s*([-+]?\d+\.\d+),([-+]?\d+\.\d+)", line)
            if match:
                lat = float(match.group(1))
                lon = float(match.group(2))

                if tempo_min_lat <= lat <= tempo_max_lat and tempo_min_lon <= lon <= tempo_max_lon:
                    outfile.write(line)

if __name__ == "__main__":
    filter_locations("./openaq_locations.txt", "./filtered_openaq_locations.txt")


