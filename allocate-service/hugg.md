# Настройка репы Huggingface

Экспорт прокси 
```bash
export HF_ENDPOINT="https://nexus.sanich.tech/repository/huggingface-proxy/"
export HF_HUB_DOWNLOAD_TIMEOUT=120
export HF_HUB_ETAG_TIMEOUT=1800
```

Установка зависимостей
```bash
pip install huggingface_hub    
```


Код для скачивания
```python
from huggingface_hub import snapshot_download

snapshot_download(
    repo_id="unstructuredio/yolo_x_layout",
    local_dir="./models/yolo_x_layout"
)
```


Вывод
```
e/gitlab/testhug.py
.gitattributes: 100%|██████████████████████████████████████████████████| 1.48k/1.48k [00:00<00:00, 4.84MB/s]
label_map.json: 100%|███████████████████████████████████████████████████████| 188/188 [00:00<00:00, 572kB/s]
LICENSE.txt: 100%|█████████████████████████████████████████████████████| 11.5k/11.5k [00:00<00:00, 29.8MB/s]
yolox_tiny.onnx: 100%|█████████████████████████████████████████████████| 20.2M/20.2M [00:01<00:00, 16.0MB/s]
yolox_l0.05_quantized.onnx: 100%|██████████████████████████████████████| 54.6M/54.6M [00:02<00:00, 23.6MB/s]
yolox_l.pt: 100%|████████████████████████████████████████████████████████| 218M/218M [00:04<00:00, 50.8MB/s]
yolox_l0.05.onnx: 100%|██████████████████████████████████████████████████| 217M/217M [00:04<00:00, 51.1MB/s]
Fetching 7 files: 100%|███████████████████████████████████████████████████████| 7/7 [00:04<00:00,  1.51it/s]
yolox_l0.05.onnx:  24%|███████████▊                                     | 52.4M/217M [00:01<00:04, 33.5MB/s]
```