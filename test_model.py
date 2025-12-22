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

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig
import time
#import bitsandbytes as bnb # Import bitsandbytes

def setup_model_and_tokenizer(model_id: str):

    

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

def generate_response(model, tokenizer, prompt: str, max_new_tokens: int = 512) -> str:

    if model is None or tokenizer is None:
        return "Error: Model or tokenizer is not loaded."

    messages = [{"role": "user", "content": prompt}]
    text = tokenizer.apply_chat_template(
        messages, tokenize=False, add_generation_prompt=True
    )
    inputs = tokenizer(text, return_tensors="pt").to(model.device)

    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=max_new_tokens,
            pad_token_id=tokenizer.eos_token_id
        )

    prompt_length = inputs.input_ids.shape[1]
    decoded_output = tokenizer.decode(
        outputs[0][prompt_length:],
        skip_special_tokens=True
    )
    return decoded_output
MODEL_ID = r"C:/Users/E00868/Downloads/QWEN3"

model, tokenizer = setup_model_and_tokenizer(MODEL_ID)

if model and tokenizer:
    promptengineer=r"""You are an expert SRE (Site Reliability Engineering) alert classification bot. Your sole purpose is to analyze an incoming alert (email subject or body) and output a single, minified JSON object.

The JSON output MUST have this exact structure: {"priority": "P", "trigger_name": "T", "resource_name": "R", "type": "T_TYPE"}

Instructions:

Analyze the [ALERT_DATA] provided at the end.

Find the resource_name (the computer, machine, or server name). If no specific resource is found, use "N/A".

Identify the trigger_name (the specific alert, like "Computer Down" or "Low disk space").

Use the ## Logic Rules below to determine the priority and type.

Logic Rules
1. Priority: "P1" / Type: "actionable"
(These are for immediate action, indicating outages, service stops, or critical thresholds.)

Triggers:

"Computer Down" (e.g., "CITRIX Machines: Computer Down SDN_TS SGD", "SAP-PP Machines: Computer Down")

EXCEPTION: If the text contains "Machine shutdown gracefully", it is NOT P1. Treat it as informational.

"Service Down"

"Service Stopped" (e.g., "OI-RDA - WIS-Service Stopped", "CITRIX PVS Services down", "SAP-SW Service Jira Tomcat Down")

"CPU utilization ≥ 95%"

"Memory utilization ≥ 95%"

"Disk queue length ≥ 1" (or "≥ 21" for backup servers)

"vCPU/pCPU ratio ≥ 3"

"Storage latency ≥ 100 ms"

"ADC Storefront LB degraded"

"ADC Exchange LB RPC/Active Sync/OWA degraded"

"ADC WEM Services LB degraded"

"FSLogix Profile corrupted"

"FSLogix Error"

"Broker cannot find any available VM"

"WinLogon Error"

"XenDesktop Error Events"

2. Priority: "P2" / Type: "actionable"
(These are for non-urgent issues that still require action, like performance or capacity warnings.)

Triggers:

"Low disk space"

"Less than 5 GB"

"Less than 15 GB"

"Less than 10% free capacity"

"Linux Disk less 20% free"

"VDA: D:\ Drive Free Space ≤ 2 GB"

"Hypervisor Memory Ballooning ≥ 10 MB"

"OI-IBS Exchange Memory/CPU Monitor"

"Application Servers Less 5GB AND less 10 Percent"

Any "Service Up" or "restored" event (e.g., "ADC Exchange LB RPC restored", "OI-RDA - WIS-Service Started", "CITRIX License Service up")

3. Priority: "informational" / Type: "informational"
(These are for logging, status checks, or monitoring without immediate risk.)

Triggers:
Any "Machine shut down gracefully" is an informational.

When email subject says “Machine shutdown gracefully”, then the shutdown was planned or manually done by an admin. If so, no immediate action is necessary. 

"Windows Event matching a custom filter"

"Service Trigger Scoutbees-Services"

"Service Trigger GLT-Services"

"ADC Certificate expiration warning"

"Citrix Storefront Test"

"Citrix XenApp Restore"

"VMware Horizon Connection Server Health Monitoring"

"VMware Horizon Connection Server Events"

"VMware Horizon Connection Server Process Monitoring"

"Linux Xterm Process Ended"

"SAP-BASIS Proc ended BELLIN Client Transport.exe"

"BITZER – Machine Down matching a custom filter"

"ADC NetScaler Schwenk"

"OI-IBS Exchange DB Schwenk"

"OI-IBS-OWI Printserver Event ID 4009"

"OI-IBS Exchange Fehler Zustellung Event IDs 15004/15006"

Task: Analyze the alert data below. Respond ONLY with the single, minified JSON object. Do not provide any other text, explanation, or formatting.

[ALERT_DATA]"""
    summerizeprompt="Give a summerized description the below data for a jira ticket by giving details of resource/machine/computer name for which it got triggered and trigger name and also summerize the cause and action recommended if there is a problem with the system but donot give action reccomendation if the email is just informational which is like a service restart or start. I need a specific elaborate summary section in the output for all the responses"
    prompt =rf"""{promptengineer}
Subject: ControlUp alert mail - Advanced Trigger -  Logical Disk: C:\ on Computer: DESDN04199.bitzer.biz. Body: This is an automated alert from ControlUp, which you requested to receive using an incident trigger.
The monitored resource has matched the conditions specified below.Organization Name: Bitzer
Folder name: bitzer\citrix\vda\vdi
     Reported by: ControlUp Monitor Service on DESDN01097
     Trigger name: CITRIX Machines: Less 5GB AND less 10 Percent <controlup://incidents/CITRIX Machines: Less 5GB AND less 10 Percent>
     Resource name: Logical Disk: C:\ on Computer: DESDN04199.bitzer.biz. <controlup://incidents/Logical Disk: C:\ on Computer: DESDN04199.bitzer.biz.>
     Resource type: LogicalDisks
     Stress Level: Advanced
     Incident timestamp (UTC +2 W. Europe Standard Time): 8/27/2025 12:09:29 PM
     Columns involved in this incident:
      Column: Free Space <controlup://incidents/Free Space>
      Value changed from 5 (GB) to 4.9 (GB)
      On (UTC +2 W. Europe Standard Time): 8/27/2025 12:04:29 PM
      Threshold crossed: 5 (GB)
      Column: % Free Space <controlup://incidents/% Free Space>
      Value is 5%
      On (UTC +2 W. Europe Standard Time): 8/27/2025 12:04:29 PM
      Threshold crossed: 10%
In order to configure which e-mail alerts you receive, please open ControlUp and edit the settings for the trigger specified above."""
    #print(f"\nUser Prompt: {prompt}\n")
    response = generate_response(model, tokenizer, prompt)
    print(f"Model Response:\n{response}")
    prompt=rf"""{summerizeprompt}Subject: ControlUp alert mail - Advanced Trigger -  Logical Disk: C:\ on Computer: DESDN04199.bitzer.biz. Body: This is an automated alert from ControlUp, which you requested to receive using an incident trigger.
The monitored resource has matched the conditions specified below.Organization Name: Bitzer
Folder name: bitzer\citrix\vda\vdi
     Reported by: ControlUp Monitor Service on DESDN01097
     Trigger name: CITRIX Machines: Less 5GB AND less 10 Percent <controlup://incidents/CITRIX Machines: Less 5GB AND less 10 Percent>
     Resource name: Logical Disk: C:\ on Computer: DESDN04199.bitzer.biz. <controlup://incidents/Logical Disk: C:\ on Computer: DESDN04199.bitzer.biz.>
     Resource type: LogicalDisks
     Stress Level: Advanced
     Incident timestamp (UTC +2 W. Europe Standard Time): 8/27/2025 12:09:29 PM
     Columns involved in this incident:
      Column: Free Space <controlup://incidents/Free Space>
      Value changed from 5 (GB) to 4.9 (GB)
      On (UTC +2 W. Europe Standard Time): 8/27/2025 12:04:29 PM
      Threshold crossed: 5 (GB)
      Column: % Free Space <controlup://incidents/% Free Space>
      Value is 5%
      On (UTC +2 W. Europe Standard Time): 8/27/2025 12:04:29 PM
      Threshold crossed: 10%
In order to configure which e-mail alerts you receive, please open ControlUp and edit the settings for the trigger specified above."""
    response = generate_response(model, tokenizer, prompt)
    print(f"Model Response:\n{response}")

    input2 = rf"""{promptengineer}   Subject: ControlUp alert mail - Process Started -  Process 3323 on DESDN01011 (StreamService.exe) Body: 
A process was started on one of your managed computers.
     Organization Name: Bitzer
     Process name: StreamService.exe <controlup://incidents/StreamService.exe>
     Process ID: 3322
     User name: BITZER\s00228 <controlup://incidents/BITZER\s00228>
     Session ID: 0
     Computer name: DESDN01011 <controlup://incidents/DESDN01057>
     Incident timestamp (UTC +2 W. Europe Standard Time): 8/27/2025 8:29:21 AM
     Trigger name: CITRIX PVS Service up <controlup://incidents/CITRIX PVS Service up>

In order to configure which e-mail alerts you receive, please open ControlUp and edit the settings for the trigger specified above.


"""
    
    prompt = input2
    response = generate_response(model, tokenizer, prompt)
    print(f"Model Response:\n{response}")
    prompt=rf"""{summerizeprompt}Subject: ControlUp alert mail - Process Started -  Process 3323 on DESDN01011 (StreamService.exe) Body: 
A process was started on one of your managed computers.
     Organization Name: Bitzer
     Process name: StreamService.exe <controlup://incidents/StreamService.exe>
     Process ID: 3322
     User name: BITZER\s00228 <controlup://incidents/BITZER\s00228>
     Session ID: 0
     Computer name: DESDN01011 <controlup://incidents/DESDN01057>
     Incident timestamp (UTC +2 W. Europe Standard Time): 8/27/2025 8:29:21 AM
     Trigger name: CITRIX PVS Service up <controlup://incidents/CITRIX PVS Service up>

In order to configure which e-mail alerts you receive, please open ControlUp and edit the settings for the trigger specified above.
"""
    response = generate_response(model, tokenizer, prompt)
    print(f"Model Response:\n{response}")
    input3=rf"""{promptengineer}Subject: ControlUp alert mail - Advanced Trigger -  Machine DESDN04101T.bitzer.biz, Body: "=This is an automated alert from ControlUp, which you requested to receive using an incident trigger. \r\n\r\n\t\r\nThe monitored resource has matched the conditions specified below.\r\n\r\n\r\n     Organization Name: Bitzer\r\n\r\n\r\n     Folder name: bitzer\\citrix\\vda\\vdi\r\n\r\n\r\n     Reported by: ControlUp Monitor Service on DESDN01126\r\n\r\n\r\n     Trigger name: CITRIX Machines: Disk Queue greater than or equal 1 <controlup://incidents/CITRIX Machines: Disk Queue greater than or equal 1> \r\n\r\n\r\n     Resource name: DESDN04101T <controlup://incidents/DESDN04101T> \r\n\r\n\r\n     Resource type: Computers\r\n\r\n\r\n     Stress Level: Advanced\r\n\r\n\r\n     Incident timestamp (UTC +1 W. Europe Standard Time): 11/13/2025 7:16:35 AM\r\n\r\n\r\n     Columns involved in this incident:\r\n\r\n\r\n      Column: Disk Queue <controlup://incidents/Disk Queue> \r\n\r\n\r\n      Value changed from 0 to 3.6\r\n\r\n\r\n      On (UTC +1 W. Europe Standard Time): 11/13/2025 7:11:35 AM\r\n\r\n\r\n      Threshold crossed: 1\r\n\r\n1 \r\nIn order to configure which e-mail alerts you receive, please open ControlUp and edit the settings for the trigger specified above.\r\n\r\n"""
    prompt = input3
    response = generate_response(model, tokenizer, prompt)
    print(f"Model Response:\n{response}")
    prompt=rf"""{summerizeprompt}Subject: ControlUp alert mail - Advanced Trigger -  Machine DESDN04101T.bitzer.biz, Body: "=This is an automated alert from ControlUp, which you requested to receive using an incident trigger. \r\n\r\n\t\r\nThe monitored resource has matched the conditions specified below.\r\n\r\n\r\n     Organization Name: Bitzer\r\n\r\n\r\n     Folder name: bitzer\\citrix\\vda\\vdi\r\n\r\n\r\n     Reported by: ControlUp Monitor Service on DESDN01126\r\n\r\n\r\n     Trigger name: CITRIX Machines: Disk Queue greater than or equal 1 <controlup://incidents/CITRIX Machines: Disk Queue greater than or equal 1> \r\n\r\n\r\n     Resource name: DESDN04101T <controlup://incidents/DESDN04101T> \r\n\r\n\r\n     Resource type: Computers\r\n\r\n\r\n     Stress Level: Advanced\r\n\r\n\r\n     Incident timestamp (UTC +1 W. Europe Standard Time): 11/13/2025 7:16:35 AM\r\n\r\n\r\n     Columns involved in this incident:\r\n\r\n\r\n      Column: Disk Queue <controlup://incidents/Disk Queue> \r\n\r\n\r\n      Value changed from 0 to 3.6\r\n\r\n\r\n      On (UTC +1 W. Europe Standard Time): 11/13/2025 7:11:35 AM\r\n\r\n\r\n      Threshold crossed: 1\r\n\r\n1 \r\nIn order to configure which e-mail alerts you receive, please open ControlUp and edit the settings for the trigger specified above.\r\n\r\n"""
    response = generate_response(model, tokenizer, prompt)
    print(f"Model Response:\n{response}")
    input4=rf"""{promptengineer} Subject: ControlUp alert mail - Machine Down -  Machine DESKZ02550.bitzer.biz is down (Machine shut down gracefully.), Body: This is an automated alert from ControlUp, which you requested to receive using an incident trigger. \r\n\r\n\t\r\nOne of your managed computers has disconnected from monitoring.\r\n\r\n\r\n     Organization name: Bitzer\r\n\r\n\r\n     Trigger name: OI-IBS-OWI Machines: Computer Down <controlup://incidents/OI-IBS-OWI Machines: Computer Down> \r\n\r\n\r\n     Computer name: DESKZ02550 <controlup://incidents/DESKZ02550> \r\n\r\n\r\n     Disconnect reason: Machine shut down gracefully.\r\n\r\n\r\n     Incident timestamp (UTC +1 W. Europe Standard Time): 11/13/2025 11:10:29 AM\r\n\r\n\r\n\r\nIn order to configure which e-mail alerts you receive, please open ControlUp and edit the settings for the trigger specified above.\r\n\r\n"""
    prompt = input4
    response = generate_response(model, tokenizer, prompt)
    print(f"Model Response:\n{response}")
    input4=rf"""{summerizeprompt} Subject: ControlUp alert mail - Machine Down -  Machine DESKZ02550.bitzer.biz is down (Machine shut down gracefully.), Body: This is an automated alert from ControlUp, which you requested to receive using an incident trigger. \r\n\r\n\t\r\nOne of your managed computers has disconnected from monitoring.\r\n\r\n\r\n     Organization name: Bitzer\r\n\r\n\r\n     Trigger name: OI-IBS-OWI Machines: Computer Down <controlup://incidents/OI-IBS-OWI Machines: Computer Down> \r\n\r\n\r\n     Computer name: DESKZ02550 <controlup://incidents/DESKZ02550> \r\n\r\n\r\n     Disconnect reason: Machine shut down gracefully.\r\n\r\n\r\n     Incident timestamp (UTC +1 W. Europe Standard Time): 11/13/2025 11:10:29 AM\r\n\r\n\r\n\r\nIn order to configure which e-mail alerts you receive, please open ControlUp and edit the settings for the trigger specified above.\r\n\r\n"""
    prompt = input4
    response = generate_response(model, tokenizer, prompt)
    print(f"Model Response:\n{response}")