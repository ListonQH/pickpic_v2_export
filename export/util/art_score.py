import io
import os
import json
import numpy as np
import torch
import pytorch_lightning as pl
import torch.nn as nn
from torchvision import datasets, transforms
from tqdm import tqdm
from os.path import join
from datasets import load_dataset
from torch.utils.data import Dataset, DataLoader
import json
import clip
from PIL import Image, ImageFile

device = "cuda" if torch.cuda.is_available() else "cpu"
print('device:', device)

#####  This script will predict the aesthetic score for this image file:
img_path = "test.png"

# if you changed the MLP architecture during training, change it also here:
class MLP(pl.LightningModule):
    def __init__(self, input_size, xcol='emb', ycol='avg_rating'):
        super().__init__()
        self.input_size = input_size
        self.xcol = xcol
        self.ycol = ycol
        self.layers = nn.Sequential(
            nn.Linear(self.input_size, 1024),
            #nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(1024, 128),
            #nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(128, 64),
            #nn.ReLU(),
            nn.Dropout(0.1),

            nn.Linear(64, 16),
            #nn.ReLU(),

            nn.Linear(16, 1)
        )

    def forward(self, x):
        return self.layers(x)

    def training_step(self, batch, batch_idx):
            x = batch[self.xcol]
            y = batch[self.ycol].reshape(-1, 1)
            x_hat = self.layers(x)
            loss = F.mse_loss(x_hat, y)
            return loss
    
    def validation_step(self, batch, batch_idx):
        x = batch[self.xcol]
        y = batch[self.ycol].reshape(-1, 1)
        x_hat = self.layers(x)
        loss = F.mse_loss(x_hat, y)
        return loss

    def configure_optimizers(self):
        optimizer = torch.optim.Adam(self.parameters(), lr=1e-3)
        return optimizer

def normalized(a, axis=-1, order=2):
    import numpy as np  # pylint: disable=import-outside-toplevel

    l2 = np.atleast_1d(np.linalg.norm(a, order, axis))
    l2[l2 == 0] = 1
    return a / np.expand_dims(l2, axis)

model = MLP(768)  # CLIP embedding dim is 768 for CLIP ViT L 14

s = torch.load("/dfs/data/improved-aesthetic-predictor/sac+logos+ava1-l14-linearMSE.pth")   # load the model you trained previously or the model available in this repo

model.load_state_dict(s)
model.to(device)
model.eval()

model2, preprocess = clip.load(name = "/dfs/data/pths/img_score/ViT-L-14.pt", device=device)  #RN50x64   

jsons_root_path = '/dfs/data/sketch_data/clear_version/jsons/'
imgs_root_path = '/dfs/data/sketch_data/clear_version/imgs/'
work_index = 8

err_log_file = open(f"art_score_err_log_{work_index}.txt", 'w')

check_point = 'train-00548-of-00645-4f298f282537260b'
begin_point = None

work_dir_name_list=[]
for j_name in os.listdir(jsons_root_path):
    if j_name.endswith('.json'):
        temp_dir_name = j_name.split('.')[0]
        if (int(temp_dir_name.split('-')[1]) % 10) == work_index:
            work_dir_name_list.append(temp_dir_name)

if check_point is not None:
    print('*'*50)
    print('Begin at check_point: ', check_point)
    for i, f_n in enumerate(work_dir_name_list):
        if f_n == check_point:
            begin_point = i
            work_dir_name_list = work_dir_name_list[begin_point:]
            break
    print('work list: ')
    print('\n'.join(work_dir_name_list))
    print('*'*50)


for i, work_dir in enumerate(work_dir_name_list):
    print(f"{i+1}/{len(work_dir_name_list)}: {work_dir}")  

    art_score_json = open(f'/dfs/data/sketch_data/clear_version/py_art_score/art_score_{work_dir}.json', 'w')
    with open(f"{jsons_root_path}{work_dir}.parquet.json", 'r') as f:
        lines = f.readlines()
        for line in tqdm(lines):    
            info = json.loads(line)
            temp_dict = {}
            img_name = info['0_save_name']
            temp_dict['img_name'] = img_name
            img_path = f"{imgs_root_path}{work_dir}/{img_name}"
            try:
                pil_image = Image.open(img_path)
                image = preprocess(pil_image).unsqueeze(0).to(device)
                with torch.no_grad():
                    image_features = model2.encode_image(image)
                    im_emb_arr = normalized(image_features.cpu().detach().numpy() )
                    prediction = model(torch.from_numpy(im_emb_arr).to(device).type(torch.cuda.FloatTensor))
                temp_dict['art_score'] = prediction.item()
            except Exception as ex:
                err_log_file.write(f"{work_dir_name_list}#{line}#{0}#{ex}")
                temp_dict['art_score'] = -1

            json_str = json.dumps(temp_dict)
            art_score_json.write(json_str)
            art_score_json.write('\n')
            art_score_json.flush()

            temp_dict = {}
            img_name = info['1_save_name']
            temp_dict['img_name'] = img_name
            img_path = f"{imgs_root_path}{work_dir}/{img_name}"
            try:
                pil_image = Image.open(img_path)
                image = preprocess(pil_image).unsqueeze(0).to(device)
                with torch.no_grad():
                    image_features = model2.encode_image(image)
                    im_emb_arr = normalized(image_features.cpu().detach().numpy() )
                    prediction = model(torch.from_numpy(im_emb_arr).to(device).type(torch.cuda.FloatTensor))
                temp_dict['art_score'] = prediction.item()
            except Exception as ex:
                err_log_file.write(f"{work_dir_name_list}#{line}#{1}#{ex}")
                temp_dict['art_score'] = -1
            json_str = json.dumps(temp_dict)
            art_score_json.write(json_str)
            art_score_json.write('\n')
            art_score_json.flush()
print('*'*50)
print('\t\t Art Score Group: ', work_index, ' Done.')
print('*'*50)
