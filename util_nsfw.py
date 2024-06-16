
def key_word_checker(prompt:str):
    prompt = prompt.lower().replace(" ", "")    
    if ('xijinping' in prompt):
        return False
    return True
