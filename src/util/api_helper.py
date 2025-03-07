import os  
import base64
from openai import AzureOpenAI
import src.settings.constants as const

endpoint = os.getenv("ENDPOINT_URL", "https:vvv/")  
deployment = os.getenv("DEPLOYMENT_NAME", "gpt-4o-mini") 
subscription_key = os.getenv("AZURE_OPENAI_API_KEY", "")   
# Initialize Azure OpenAI Service client with key-based authentication    


def get_response_from_openai_json(prompt :str):
	client = AzureOpenAI(  
    azure_endpoint=endpoint,  
    api_key=subscription_key,  
    api_version="2024-05-01-preview",
)
	completion = client.chat.completions.create(
		model=deployment,
		messages=[
			{"role": "system", "content": "You are a helpful assistant, skilled in extract information from context."},
			{"role": "user", "content": prompt}
		],
		temperature=const.MODEL_TEMP
	)
	rsp = completion.choices[0].message.content
	rsp = rsp.replace("```json","")
	rsp = rsp.replace("```","")
	return rsp