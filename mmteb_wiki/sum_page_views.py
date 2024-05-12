import os
import json

def aggregate_views(start_year, end_year):
    for year in range(start_year, end_year + 1):
        dir_path = f"data/pageviews_summary/{year}"
        output_path = f"data/pageviews_summary/total_views_{year}.json"
        
        total_views = {}
        
        # Ensure the directory exists
        if not os.path.exists(dir_path):
            print(f"No data for year {year}")
            continue
        
        # List and sort all JSON files in the directory
        filenames = sorted([f for f in os.listdir(dir_path) if f.endswith('.json')])
        
        # Process each file
        for filename in filenames:
            file_path = os.path.join(dir_path, filename)
            with open(file_path, 'r') as file:
                data = json.load(file)
            
            # Aggregate views by language
            for lang, views_dict in data.items():
                if lang not in total_views:
                    total_views[lang] = {}
                for title, views in views_dict.items():
                    if title not in total_views[lang]:
                        total_views[lang][title] = 0
                    total_views[lang][title] += views
            
                print(f"{filename}")
                print(total_views[lang]["Berlin"])
        
        # Write the aggregated data to a file
        with open(output_path, 'w') as file:
            json.dump(total_views, file, indent=4)
        
        print(f"Aggregated data written to {output_path}")

def aggregate_final_total_views(start_year, end_year):
    final_total_views = {}
    base_dir = "data/pageviews_summary"

    for year in range(start_year, end_year + 1):
        input_path = os.path.join(base_dir, f"total_views_{year}.json")
        
        if not os.path.exists(input_path):
            print(f"No aggregated data for year {year}")
            continue
        
        with open(input_path, 'r') as file:
            yearly_data = json.load(file)
        
        # Aggregate views by language across all years
        for lang, views_dict in yearly_data.items():
            if lang not in final_total_views:
                final_total_views[lang] = {}
            for title, views in views_dict.items():
                if title not in final_total_views[lang]:
                    final_total_views[lang][title] = 0
                final_total_views[lang][title] += views
            print(final_total_views[lang]["Berlin"])

    # Write the final aggregated data to a file
    final_output_path = os.path.join(base_dir, "final_total_views.json")
    with open(final_output_path, 'w') as file:
        json.dump(final_total_views, file, indent=4)
    
    print(f"Final aggregated data written to {final_output_path}")

# aggregate_views(2015, 2024)
aggregate_final_total_views(2015, 2024)