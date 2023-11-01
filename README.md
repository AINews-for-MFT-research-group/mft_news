# mft_news

Code for "One Consensus, Diverse Expressions：Ethical Spectrum Analysis of the 'Carbon' Issue in the Global News Database" (submitted to ICA2024)

## Code

All codes are in Python.

Details:

- config.py
    - Configurations for MySQL and Zhipuai API
- get_page.py
    - Page content crawler
- build_datasets.py
    - Build datasets based on the MFT Dictionaries.
- build_itemdata.py
    - Get NER results from Zhipuai
- get_mft.py
    - Get Mft results from Zhipuai



## Prompts

- prompt_getitem
    - Extract NER items. 

- prompt_mft_new
    - Get Mft results based on the new method (with 3 categories).

- prompt_mft_old
    - Get Mft results based on the tradional method (with 5 categories).

## Datasets
Datasets can be found in https://huggingface.co/datasets/school-knight/MFT_NEWS
