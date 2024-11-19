# from transformers import AutoTokenizer
# from vllm import LLM, SamplingParams
# from PIL import Image
import dts_helper
import time

login_result = None

# model_name = "C:/code/internvl/pretrained/InternVL2-2B"
# llm = LLM(
#         model=model_name,
#         trust_remote_code=True,
#         max_model_len=4096,
#     )

# tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)

# question=""
# messages = [{'role': 'user', 'content': f"<image>\n{question}"}]

# prompt = tokenizer.apply_chat_template(messages,
#                                         tokenize=False,
#                                         add_generation_prompt=True)

# stop_tokens = ["<|endoftext|>", "<|im_start|>", "<|im_end|>", "<|end|>"]
# stop_token_ids = [tokenizer.convert_tokens_to_ids(i) for i in stop_tokens]

# sampling_params = SamplingParams(temperature=0.2,
#                                      max_tokens=64,
#                                      stop_token_ids=stop_token_ids)

def vllm_inference(work_sapce, cuda_name):
    global login_result
    login_result = dts_helper.login(work_sapce, cuda_name)
    if login_result == None:
        return
    while True:
        work_list = dts_helper.get_imgs_path_list(login_result,5)
        if len(work_list) == 0:
            print('[ info ] vllm_inference: get empty work list!')
            break
        # 组装 batch，开始推理
        inputs = []
        print('[ info ] work_list size=', len(work_list))
        for img_path in work_list:
            print(f'internVL-8B: [{img_path}]')
            time.sleep(0.5)

if __name__=='__main__':
    print('Are u OK?')
    work_space = 'win10'
    cuda_name = 'cuda:0'
    vllm_inference(work_sapce=work_space, cuda_name=cuda_name)