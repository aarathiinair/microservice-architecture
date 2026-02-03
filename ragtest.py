import pandas as pd
import json
import torch
import os
from langchain_community.vectorstores import FAISS
from langchain_core.documents import Document

from transformers import AutoModel, AutoModelForCausalLM, AutoTokenizer
# then define a dummy class if neither exists (duck typing).
try:
    from langchain_core.embeddings import Embeddings
except ImportError:
    try:
        from langchain.embeddings.base import Embeddings
    except ImportError:
        class Embeddings:
            """Dummy base class if LangChain is too old"""
            pass

EMBEDDING_MODEL_PATH = r"C:\Users\E00867\Downloads\sentence"  # or your local path like "/models/bert-base"
LLM_MODEL_PATH = r"C:\Users\E00867\Downloads\Qwen_fine_tuned"                      # or your local path like "/models/qwen-local"
CSV_FILE_PATH = "./ControlUp Trigger Details.xlsx"
VECTOR_STORE_PATH = "./faiss_index_manual"
device = "cuda" if torch.cuda.is_available() else "cpu"
class CustomHFEmbeddings(Embeddings):
    """
    A robust wrapper for AutoModel to work with FAISS.
    Fixed: Inherits from Embeddings and implements __call__ to prevent TypeError.
    """
    def __init__(self, model_path, device):
        print(f"--- Loading Embedding Model from: {model_path} ---")
        self.tokenizer = AutoTokenizer.from_pretrained(model_path)
        self.model = AutoModel.from_pretrained(model_path).to(device)
        self.device = device

    def _compute_vectors(self, texts):
        # Ensure input is a list
        if isinstance(texts, str):
            texts = [texts]
            
        # Tokenize
        inputs = self.tokenizer(
            texts, 
            padding=True, 
            truncation=True, 
            max_length=512, 
            return_tensors="pt"
        ).to(self.device)

        # Forward pass
        with torch.no_grad():
            outputs = self.model(**inputs)

        # Mean Pooling
        token_embeddings = outputs.last_hidden_state
        input_mask_expanded = inputs['attention_mask'].unsqueeze(-1).expand(token_embeddings.size()).float()
        sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, 1)
        sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)
        embeddings = sum_embeddings / sum_mask

        # Normalize
        embeddings = torch.nn.functional.normalize(embeddings, p=2, dim=1)
        
        # Convert to list of lists
        return embeddings.cpu().numpy().tolist()

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        return self._compute_vectors(texts)

    def embed_query(self, text: str) -> List[float]:
        # Helper: embed_query usually expects a single vector (list of floats)
        return self._compute_vectors([text])[0]
    
    # --- CRITICAL FIX: Make the object callable ---
    def __call__(self, text: str) -> List[float]:
        return self.embed_query(text)

# ==========================================
# 3. Build & Save Vector Store
# ==========================================
def build_knowledge_base():
    print("--- Reading CSV Data ---")
    df = pd.read_excel(CSV_FILE_PATH)
    df = df.fillna("N/A")

    documents = []
    for index, row in df.iterrows():
        # 1. Search Content: ONLY the Trigger Name (for precision)
        search_content = str(row['TriggerName'])
        
        # 2. Metadata: The FULL ROW (for the answer)
        full_row_data = row.to_dict()
        
        doc = Document(
            page_content=search_content,
            metadata={"row_data": json.dumps(full_row_data)} # Store as string to be safe
        )
        documents.append(doc)

    # Initialize Custom Embeddings
    embedding_handler = CustomHFEmbeddings(EMBEDDING_MODEL_PATH, device)

    print(f"--- Creating FAISS Index with {len(documents)} items ---")
    vector_store = FAISS.from_documents(documents, embedding_handler)
    
    # Save to disk
    vector_store.save_local(VECTOR_STORE_PATH)
    print(f"--- Index saved to {VECTOR_STORE_PATH} ---")

# ==========================================
# 4. Retrieval & Generation
# ==========================================
def process_trigger(trigger_query ,subject):
    # --- A. Load Resources ---
    if not os.path.exists(VECTOR_STORE_PATH):
        print("Index not found, building it now...")
        build_knowledge_base()

    embedding_handler = CustomHFEmbeddings(EMBEDDING_MODEL_PATH, device)
    vector_store = FAISS.load_local(
        VECTOR_STORE_PATH, 
        embedding_handler, 
        allow_dangerous_deserialization=True
    )

    # --- B. Retrieve Exact Row ---
    print(f"\nSearching for: '{trigger_query}'")
    docs = vector_store.similarity_search(trigger_query, k=1)
    
    if not docs:
        print("No matching trigger found.")
        return

    # Extract the full row data
    retrieved_doc = docs[0]
    selected_row_json = retrieved_doc.metadata['row_data']
    print(f"Retrieved Match: {retrieved_doc.page_content}")

    # --- C. Load LLM (AutoModelForCausalLM) ---
    print(f"--- Loading LLM: {LLM_MODEL_PATH} ---")
    tokenizer = AutoTokenizer.from_pretrained(LLM_MODEL_PATH, trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(
        LLM_MODEL_PATH, 
        device_map="auto", 
        torch_dtype=torch.float16,
        trust_remote_code=True
    )

    # --- D. Generate JSON ---
    prompt = f"""<|im_start|>system
You are a JSON extractor. Output valid JSON only.<|im_end|>
<|im_start|>user


Subject: "{subject}"
Query: "{trigger_query}"

### Instructions:
Extract the following fields into a JSON object. 

**Special Override Rule:** If the **Subject** contains "machine shutdown gracefully":
- Ignore the values in the "Context (Database Row)".
- Set "priority" to "informational".
- Set "type" to "informational".
- Set "recommended_action" to "N/A".
- If it GOES in the special override rule, create key value pair field called "Override" and set value as "TRUE"
- If it DOESN'T GO to the special override rule, create key value pair field called "Override" and set value as "FALSE"
Context (Database Row):
{selected_row_json}

Otherwise, extract "priority", "type", and "recommended_action" directly from the provided Context row.


### Fields to Extract:
- "subject": (The value of the subject)
- "trigger_name": (The name of the trigger from the query or row)
- "priority": (The priority level)
- "type": (The value from 'Informational/Actionable')
- "recommended_action": (The value from 'Recommended Actions')
- "selected_row": (The entire Context row provided above)

Ensure the output is strictly valid JSON.
<|im_end|>
<|im_start|>assistant
"""

    inputs = tokenizer(prompt, return_tensors="pt").to(device)
    
    with torch.no_grad():
        generated_ids = model.generate(
            **inputs,
            max_new_tokens=512,
            temperature=0.1, 
            do_sample=False
        )
    
    output_text = tokenizer.decode(generated_ids[0][inputs.input_ids.shape[1]:], skip_special_tokens=True)
    
    print("\n--- Final JSON Output ---")
    print(output_text)
    return 

# ==========================================
# 5. Run
# ==========================================
if __name__ == "__main__":
    # Example Trigger
    input_trigger = "SAP-BASIS Proc ended BELLIN Client Transport.exe"
    subject="ControlUp alert mail - Machine Down -  Machine DESDN16128.bitzer.biz is down (Machine shut down gracefully.)"
    
    process_trigger(input_trigger,subject)