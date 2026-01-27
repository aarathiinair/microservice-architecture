import pandas as pd
import numpy as np
# from transformers import AutoTokenizer, AutoModelForCausalLM
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    BitsAndBytesConfig,
    HfArgumentParser,
    TrainingArguments,
    pipeline,
    logging,
)
import torch
from nltk.translate.bleu_score import sentence_bleu
from rouge_score import rouge_scorer
from scipy.stats import entropy
llmmodel = r"C:/Users/E00868/Downloads/Qwen_fine_tuned"

def load_model_dep():

    model = AutoModelForCausalLM.from_pretrained(
            llmmodel,
            # quantization_config=bnb_config,
            device_map="auto",
            token = 'paste_token_here'
            # offload_dir = "True"
            )


    tokenizer = AutoTokenizer.from_pretrained("C:/Users/E00868/Downloads/QWEN3",device_map="auto", trust_remote_code=True)
    tokenizer.pad_token = tokenizer.eos_token
    tokenizer.padding_side = "right" # Fix weird overflow issue with fp16 training
    pipe = pipeline(task="text-generation", model=model, tokenizer=tokenizer, max_length=1000)
    return pipe
model_id=r"C:/Users/E00868/Downloads/QWEN3"
def load_model():
    
        

        device = torch.device("cpu")
        

        #quantization_config = BitsAndBytesConfig(
        #    load_in_8bit=True
        #)
        

        compute_dtype = torch.bfloat16

        try:
            if not torch.cuda.is_available():
                tokenizer = AutoTokenizer.from_pretrained(model_id, trust_remote_code=True)

                model = AutoModelForCausalLM.from_pretrained(
                    model_id,
                    #quantization_config=quantization_config,
                    torch_dtype=compute_dtype,
                    device_map="cpu",
                    attn_implementation="flash_attention_2",
                    trust_remote_code=True
                )
            else:
                print("Error: CUDA (GPU) is  available.")
                tokenizer = AutoTokenizer.from_pretrained(model_id, trust_remote_code=True)

                model = AutoModelForCausalLM.from_pretrained(
                    model_id,
                    #quantization_config=quantization_config,
                    torch_dtype=compute_dtype,
                    device_map="cuda:0",
                    attn_implementation="flash_attention_2",
                    trust_remote_code=True
                )
                return None, None

        except ImportError:

            print("flash_attention_2 not found. Loading with default attention.")
            model = AutoModelForCausalLM.from_pretrained(
                model_id,
                #quantization_config=quantization_config,
                torch_dtype=compute_dtype,
                device_map="cpu",
                trust_remote_code=True
            )
        except Exception as e:
            print(f"Error loading model: {e}")
            return None, None

        model.eval()
        allocated = torch.cuda.memory_allocated() / 1024**3
        reserved = torch.cuda.memory_reserved() / 1024**3
        print(f"VRAM after 8-bit load: Allocated: {allocated:.2f} GB | Reserved: {reserved:.2f} GB")

        return model, tokenizer