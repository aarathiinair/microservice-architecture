#from app.api.v1.endpoints import pipe
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig
import time
from app.logging.logging_config import model_logger
from app.logging.logging_decorator import log_function_call

class ModelProcessing():
    def __init__(self,model,tokenizer,pipe=None):
        #self.body = body
        self.model=model
        self.tokenizer=tokenizer
        self.model_pipeline=pipe
    
    @log_function_call(model_logger)
    def process(self,body):
        #global pipe
        
        if self.model is None or self.tokenizer is None:
            print("error in model")
            raise RuntimeError("The model pipeline (pipe) has not been loaded or failed during startup.")

        input1 = "subject:" +body['subject'] + "body:" + body['content']
        file_path = "C:\Email_processing_demo\segregationprompt.txt"  # Replace with your file's path

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                file_content = f.read()
           
        except FileNotFoundError:
            print(f"Error: The file at {file_path} was not found.")
        except Exception as e:
            print(f"An error occurred: {e}")

        prompt=file_content+input1
        result = self.generate_response(prompt)
        return eval(result[0]['generated_text'].split('[/INST]')[1][:-4]) if isinstance(result, list) and len(result) > 0 and isinstance(result[0], dict) and 'generated_text' in result[0] else result

    @log_function_call(model_logger)  
    def summary(self,body):
        if self.model is None or self.tokenizer is None:
            print("error in model")
            raise RuntimeError("The model pipeline (pipe) has not been loaded or failed during startup.")

        input1 = "subject:" +body['subject'] + "body:" + body['content']
        file_path = "C:\Email_processing_demo\summarizeprompt.txt"  # Replace with your file's path

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                file_content = f.read()
            

        except FileNotFoundError:
            print(f"Error: The file at {file_path} was not found.")
        except Exception as e:
            print(f"An error occurred: {e}")

        prompt=file_content+input1
        result = self.generate_response(prompt)

        return eval(result[0]['generated_text'].split('[/INST]')[1][:-4]) if isinstance(result, list) and len(result) > 0 and isinstance(result[0], dict) and 'generated_text' in result[0] else result
    
    @log_function_call(model_logger)
    def generate_response(self,prompt: str, max_new_tokens: int = 312) -> str:

        if self.model is None or self.tokenizer is None:
            return "Error: Model or tokenizer is not loaded."

        messages = [{"role": "user", "content": prompt}]
        text = self.tokenizer.apply_chat_template(
            messages, tokenize=False, add_generation_prompt=True
        )
        inputs = self.tokenizer(text, return_tensors="pt").to(self.model.device)

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


        