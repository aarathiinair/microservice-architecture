import os
import json
import time
from model import load_model
from model_processing import ModelProcessing

def main():
    # ---------------- CONFIGURATION ---------------- #
    # Path to the folder containing your JSON files
    INPUT_FOLDER = r"C:\Email_processing_demo\edge_cases" 
    
    # Ensure this matches where you actually put the prompt files
    # defined inside model_processing.py (C:\Email_processing_demo\...)
    # ----------------------------------------------- #

    print(">>> Initializing Model (this may take time)...")
    # 1. Load the model and tokenizer using your model.py function
    loaded_model, tokenizer = load_model()

    if loaded_model is None or tokenizer is None:
        print("CRITICAL ERROR: Failed to load model or tokenizer. Check model.py paths and CUDA settings.")
        return

    print(">>> Model Loaded Successfully.")

    # 2. Initialize the processing class
    # We pass None for 'pipe' as it seems unused or optional in your __init__ logic based on the usage
    processor = ModelProcessing(model=loaded_model, tokenizer=tokenizer, pipe=None)

    # 3. Process JSON files
    if not os.path.exists(INPUT_FOLDER):
        print(f"Error: Input folder not found at {INPUT_FOLDER}")
        return

    print(f"\n>>> Starting processing of files in: {INPUT_FOLDER}")

    files = [f for f in os.listdir(INPUT_FOLDER) if f.endswith('.json')]
    
    if not files:
        print("No JSON files found in the directory.")
        return

    for filename in files:
        file_path = os.path.join(INPUT_FOLDER, filename)
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            print(f"\n--------------------------------------------------")
            print(f"Processing File: {filename}")

            # 4. Data Mapping
            # Your ModelProcessing class expects a dict with keys ['subject', 'content']
            # Your JSON input has ['subject', 'body']. We must map 'body' to 'content'.
            formatted_body = {
                "subject": data.get("subject", ""),
                "content": data.get("body", "") # Mapping 'body' from JSON to 'content' for the class
            }

            # 5. Run Segregation (Process)
            # This uses C:\Email_processing_demo\segregationprompt.txt
            print("   > Running Segregation/Classification...")
            start_time = time.time()
            segregation_result = processor.process(formatted_body)
            print(f"   [Segregation Result]: {segregation_result}")
            
            # 6. Run Summarization (Summary)
            # This uses C:\Email_processing_demo\summarizeprompt.txt
            print("   > Running Summarization...")
            summary_result = processor.summary(formatted_body)
            print(f"   [Summary Result]: {summary_result}")
            
            print(f"   > Time taken: {time.time() - start_time:.2f}s")

        except json.JSONDecodeError:
            print(f"   [ERROR] File {filename} is not valid JSON.")
        except Exception as e:
            print(f"   [ERROR] Failed to process {filename}. Reason: {e}")

    print("\n>>> Processing Complete.")

if __name__ == "__main__":
    main()