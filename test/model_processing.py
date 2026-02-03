#from app.api.v1.endpoints import pipe
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig
import time
import os

class ModelProcessing():
    def __init__(self,model,tokenizer,pipe=None):
        #self.body = body
        self.model=model
        self.tokenizer=tokenizer
        self.model_pipeline=pipe

    def validate_input(self, body):
        """
        Helper method to validate input dictionary.
        Returns tuple: (is_valid, subject, content, error_message)
        """
        if not isinstance(body, dict):
            return False, None, None, "Input must be a dictionary."

        subject = body.get('subject')
        content = body.get('content')

        # Check if keys exist and are not None/Empty
        has_subject = subject is not None and str(subject).strip() != ""
        has_content = content is not None and str(content).strip() != ""

        # Logic: If it doesn't have any (neither subject nor body), throw exception
        if not has_subject and not has_content:
            raise ValueError("Invalid Input: Dictionary contains neither 'subject' nor 'content'.")

        # Logic: If has only subject (no body), don't process
        if has_subject and not has_content:
            print("Validation: Found subject but no body. Skipping processing.")
            return False, subject, content, "Skipped: Missing body."

        # If we have content, we process (even if subject is missing, we default it to empty to avoid errors)
        if not has_subject:
            subject = ""

        return True, subject, content, None

    def process(self, body):
        try:
            if self.model is None or self.tokenizer is None:
                print("Error: Model or tokenizer not initialized.")
                raise RuntimeError("The model pipeline (pipe) has not been loaded or failed during startup.")

            # --- Validation Logic ---
            is_valid, subject, content, error_msg = self.validate_input(body)
            if not is_valid:
                return error_msg # Returns None or error string based on logic
            
            # --- Processing Logic ---
            input1 = "subject:" + str(subject) + "body:" + str(content)
            file_path = r"C:\Email_processing_demo\segregationprompt.txt" # Used raw string for path

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    file_content = f.read()
            except FileNotFoundError:
                print(f"Error: The file at {file_path} was not found.")
                return "Error: Prompt file not found."
            except Exception as e:
                print(f"An error occurred reading file: {e}")
                return f"Error reading prompt file: {e}"

            prompt = file_content + input1
            result = self.generate_response(prompt)
            
            # Safe evaluation
            try:
                if isinstance(result, list) and len(result) > 0 and isinstance(result[0], dict) and 'generated_text' in result[0]:
                    # Extract text after [/INST] and remove last 4 chars as per original logic
                    split_text = result[0]['generated_text'].split('[/INST]')
                    if len(split_text) > 1:
                        return eval(split_text[1][:-4])
                    else:
                        return result[0]['generated_text']
                else:
                    return result
            except Exception as parse_error:
                print(f"Error parsing model output: {parse_error}")
                return result

        except ValueError as ve:
            print(f"Validation Error: {ve}")
            raise ve # Re-raise as requested for empty inputs
        except Exception as e:
            print(f"Critical Error in process: {e}")
            return f"Error processing request: {e}"

    def summary(self, body):
        try:
            if self.model is None or self.tokenizer is None:
                print("Error: Model or tokenizer not initialized.")
                raise RuntimeError("The model pipeline (pipe) has not been loaded or failed during startup.")

            # --- Validation Logic ---
            is_valid, subject, content, error_msg = self.validate_input(body)
            if not is_valid:
                return error_msg

            # --- Processing Logic ---
            input1 = "subject:" + str(subject) + "body:" + str(content)
            file_path = r"C:\Email_processing_demo\summarizeprompt.txt"

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    file_content = f.read()
            except FileNotFoundError:
                print(f"Error: The file at {file_path} was not found.")
                return "Error: Prompt file not found."
            except Exception as e:
                print(f"An error occurred reading file: {e}")
                return f"Error reading prompt file: {e}"

            prompt = file_content + input1
            result = self.generate_response(prompt)

            # Safe evaluation
            try:
                if isinstance(result, list) and len(result) > 0 and isinstance(result[0], dict) and 'generated_text' in result[0]:
                    split_text = result[0]['generated_text'].split('[/INST]')
                    if len(split_text) > 1:
                        return eval(split_text[1][:-4])
                    else:
                        return result[0]['generated_text']
                else:
                    return result
            except Exception as parse_error:
                print(f"Error parsing model output: {parse_error}")
                return result

        except ValueError as ve:
            print(f"Validation Error: {ve}")
            raise ve
        except Exception as e:
            print(f"Critical Error in summary: {e}")
            return f"Error processing summary: {e}"

    def generate_response(self,prompt: str, max_new_tokens: int = 312) -> str:
        try:
            if self.model is None or self.tokenizer is None:
                return "Error: Model or tokenizer is not loaded."

            messages = [{"role": "user", "content": prompt}]
            
            try:
                text = self.tokenizer.apply_chat_template(
                    messages, tokenize=False, add_generation_prompt=True
                )
                inputs = self.tokenizer(text, return_tensors="pt").to(self.model.device)
            except Exception as token_error:
                print(f"Tokenization error: {token_error}")
                return f"Error during tokenization: {token_error}"

            with torch.no_grad():
                outputs = self.model.generate(
                    **inputs,
                    max_new_tokens=max_new_tokens,
                    pad_token_id=self.tokenizer.eos_token_id
                )

            prompt_length = inputs.input_ids.shape[1]
            decoded_output = self.tokenizer.decode(
                outputs[0][prompt_length:],
                skip_special_tokens=True
            )
            return decoded_output
            
        except Exception as e:
            print(f"Error generating response: {e}")
            return f"Generation failed: {e}"