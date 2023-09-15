import csv
import os

def clean_data(data_list):
    """Replace '???' with None in the given list."""
    return [item if item != "???" else None for item in data_list]

def convert_timestamp(timestamp):
    """Convert timestamp to ISO 8601 format."""
    return timestamp.replace('_', 'T')

def add_headers_to_csv(file_path, headers):
    """Add headers to the given CSV file."""
    with open(file_path, 'r+') as f:
        content = f.read()
        f.seek(0, 0)  # Move the file pointer to the beginning of the file
        f.write(headers + "\n" + content)

def contains_keywords(line, keywords):
    """Check if the line contains all the provided keywords."""
    return all(keyword in line for keyword in keywords)

def process_log_file(input_path, output_directory):
    
    # Define the output paths based on keywords
    ato_train_path = os.path.join(output_directory, 'ato_train', os.path.basename(input_path).replace('.playback', '_ato_train.csv'))
    ato_tempzone_path = os.path.join(output_directory, 'ato_tempzone', os.path.basename(input_path).replace('.playback', '_ato_tempzone.csv'))
    ato_gate_path = os.path.join(output_directory, 'ato_gate', os.path.basename(input_path).replace('.playback', '_ato_gate.csv'))
    ato_ocs_path = os.path.join(output_directory, 'ato_ocs', os.path.basename(input_path).replace('.playback', '_ato_ocs.csv'))
    ato_trackcircuit_path = os.path.join(output_directory, 'ato_trackcircuit', os.path.basename(input_path).replace('.playback', '_ato_trackcircuit.csv'))
    ato_matrix_path = os.path.join(output_directory, 'ato_matrix', os.path.basename(input_path).replace('.playback', '_ato_matrix.csv'))
    ato_panel_path = os.path.join(output_directory, 'ato_panel', os.path.basename(input_path).replace('.playback', '_ato_panel.csv'))
    ato_rato_path = os.path.join(output_directory, 'ato_rato', os.path.basename(input_path).replace('.playback', '_ato_rato.csv'))
    ato_ratp_path = os.path.join(output_directory, 'ato_ratp', os.path.basename(input_path).replace('.playback', '_ato_ratp.csv'))
    ato_station_path = os.path.join(output_directory, 'ato_station', os.path.basename(input_path).replace('.playback', '_ato_station.csv'))
    ato_ccu_path = os.path.join(output_directory, 'ato_ccu', os.path.basename(input_path).replace('.playback', '_ato_ccu.csv'))
    ato_switch_path = os.path.join(output_directory, 'ato_switch', os.path.basename(input_path).replace('.playback', '_ato_switch.csv'))
    
    # Ensure the output directories exist
    directories = ['ato_train', 'ato_tempzone', 'ato_gate', 'ato_ocs', 'ato_trackcircuit', 'ato_matrix', 'ato_panel', 'ato_rato', 'ato_ratp', 'ato_station', 'ato_ccu', 'ato_switch'] 

    for dir_name in directories:
        dir_path = os.path.join(output_directory, dir_name)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    
    with open(input_path, 'r') as f:
        # Prepare writers for each condition
        ato_train_writer = None
        ato_tempzone_writer = None
        ato_gate_writer = None
        ato_ocs_writer = None
        ato_trackcircuit_writer = None
        ato_matrix_writer = None
        ato_panel_writer = None
        ato_rato_writer = None
        ato_ratp_writer = None
        ato_station_writer = None
        ato_ccu_writer = None
        ato_switch_writer = None
        
        content = f.read()
        
        if contains_keywords(content, ["ATO", "Train"]):
            ato_train_writer = csv.writer(open(ato_train_path, 'w', newline=''))
        if contains_keywords(content, ["ATO", "TempZone"]):
            ato_tempzone_writer = csv.writer(open(ato_tempzone_path, 'w', newline=''))
        if contains_keywords(content, ["ATO", "Gate"]):
            ato_gate_writer = csv.writer(open(ato_gate_path, 'w', newline=''))
        if contains_keywords(content, ["ATO", "OCS"]):
            ato_ocs_writer = csv.writer(open(ato_ocs_path, 'w', newline=''))
        if contains_keywords(content, ["ATO", "TrackCircuit"]):
            ato_trackcircuit_writer = csv.writer(open(ato_trackcircuit_path, 'w', newline=''))
        if contains_keywords(content, ["ATO", "Matrix"]):
            ato_matrix_writer = csv.writer(open(ato_matrix_path, 'w', newline=''))
        if contains_keywords(content, ["ATO", "Panel"]):
            ato_panel_writer = csv.writer(open(ato_panel_path, 'w', newline=''))    
        if contains_keywords(content, ["ATO", "RATO"]):
            ato_rato_writer = csv.writer(open(ato_rato_path, 'w', newline=''))
        if contains_keywords(content, ["ATO", "RATP"]):
            ato_ratp_writer = csv.writer(open(ato_ratp_path, 'w', newline=''))
        if contains_keywords(content, ["ATO", "Station"]):
            ato_station_writer = csv.writer(open(ato_station_path, 'w', newline=''))
        if contains_keywords(content, ["ATO", "CCU"]):
            ato_ccu_writer = csv.writer(open(ato_ccu_path, 'w', newline=''))
        if contains_keywords(content, ["ATO", "Switch"]):
            ato_switch_writer = csv.writer(open(ato_switch_path, 'w', newline=''))

        f.seek(0)  # Reset file pointer to the beginning
        
        for line in f:
            if line.startswith("Version"):
                continue  # skip the version line
            if "next_route_name" in line:
                continue  # Skip lines containing "next_route_name"
            if "route_name" in line:
                continue  # Skip lines containing "route_name"
            if "dsp_tdn" in line:
                continue  # Skip lines containing "dsp_tdn"
            
            # Splitting based on spaces first
            parts = line.strip().split()
            if len(parts) < 4:
                continue  # skip lines that don't have at least 5 parts
            
            # Convert timestamp to ISO 8601 format
            parts[0] = convert_timestamp(parts[0])
                
            # Splitting the third part based on dots
            sub_parts = parts[2].split('.')
            
            # Combining everything
            row_data = parts[:2] + sub_parts + parts[3:]
            cleaned_row_data = clean_data(row_data)
            
            # Check for special conditions
            if ato_train_writer and contains_keywords(line, ["ATO", "Train"]):
                ato_train_writer.writerow(cleaned_row_data)
            if ato_tempzone_writer and contains_keywords(line, ["ATO", "TempZone"]):
                ato_tempzone_writer.writerow(cleaned_row_data)
            if ato_gate_writer and contains_keywords(line, ["ATO", "Gate"]):
                ato_gate_writer.writerow(cleaned_row_data)
            if ato_ocs_writer and contains_keywords(line, ["ATO", "OCS"]):
                ato_ocs_writer.writerow(cleaned_row_data)
            if ato_trackcircuit_writer and contains_keywords(line, ["ATO", "TrackCircuit"]):
                ato_trackcircuit_writer.writerow(cleaned_row_data)
            if ato_matrix_writer and contains_keywords(line, ["ATO", "Matrix"]):
                ato_matrix_writer.writerow(cleaned_row_data)
            if ato_panel_writer and contains_keywords(line, ["ATO", "Panel"]):
                ato_panel_writer.writerow(cleaned_row_data)
            if ato_rato_writer and contains_keywords(line, ["ATO", "RATO"]):
                ato_rato_writer.writerow(cleaned_row_data)
            if ato_ratp_writer and contains_keywords(line, ["ATO", "RATP"]):
                ato_ratp_writer.writerow(cleaned_row_data)
            if ato_station_writer and contains_keywords(line, ["ATO", "Station"]):
                ato_station_writer.writerow(cleaned_row_data)
            if ato_ccu_writer and contains_keywords(line, ["ATO", "CCU"]):
                ato_ccu_writer.writerow(cleaned_row_data)    
            if ato_switch_writer and contains_keywords(line, ["ATO", "Switch"]):
                ato_switch_writer.writerow(cleaned_row_data)    

        # After processing the log file, check if the ato_train file was written and add headers
        if ato_train_writer:
            headers = "Timestamp,Source,System,Component,ID,Action,Attribute,Value,Code"
            add_headers_to_csv(ato_train_path, headers)
        if ato_switch_writer:
            headers = "Timestamp,Source,System,Component,ID,Action,Attribute,Value,Code"
            add_headers_to_csv(ato_switch_path, headers)

def main():
    # Define the output directory
    output_directory = '.'
    
    # Iterate over all txt files in the logs directory
    for log_file in os.listdir('logs'):
        if log_file.endswith('.playback'):
            input_path = os.path.join('logs', log_file)
            process_log_file(input_path, output_directory)
            print(f"Processed {input_path}")

if __name__ == '__main__':
    main()
