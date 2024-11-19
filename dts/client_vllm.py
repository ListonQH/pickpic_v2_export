from transformers import AutoTokenizer
from vllm import LLM, SamplingParams
from PIL import Image
import dts_helper

login_result = None

model_name = "C:/code/internvl/pretrained/InternVL2-2B"
llm = LLM(
        model=model_name,
        trust_remote_code=True,
        max_model_len=4096,
    )

tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)

question=""
messages = [{'role': 'user', 'content': f"<image>\n{question}"}]

prompt = tokenizer.apply_chat_template(messages,
                                        tokenize=False,
                                        add_generation_prompt=True)

stop_tokens = ["<|endoftext|>", "<|im_start|>", "<|im_end|>", "<|end|>"]
stop_token_ids = [tokenizer.convert_tokens_to_ids(i) for i in stop_tokens]

sampling_params = SamplingParams(temperature=0.2,
                                     max_tokens=64,
                                     stop_token_ids=stop_token_ids)

def vllm_inference():
    global login_result
    login_result = dts_helper.login()
    if login_result == None:
        return
    while True:
        work_list = dts_helper.get_imgs_path_list()
        if len(work_list) == 0:
            print('[ info ] vllm_inference: get empty work list!')
            break
        # 组装 batch，开始推理
        inputs = []
        for img_path in work_list:
            img = Image.open(img_path).convert("RGB")

            inputs.append({
                "prompt": prompt,
                "multi_modal_data": {
                    "image": img
                    },
            })       
            outputs = llm.generate(inputs, sampling_params=sampling_params)
            for o in outputs:
                generated_text = o.outputs[0].text
                print(generated_text)

if __name__=='__main__':
    print('Are u OK?')
