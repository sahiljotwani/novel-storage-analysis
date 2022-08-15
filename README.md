# Modelling and Analysing Novel Energy Storage Technologies in the UK Electricity Network
MSc SES Dissertation (University of Edinburgh)

## How to reproduce the results?
### Reproducing results from minimal network models:
- Jupyter notebooks available within the minimal-network-models folder
### Reproducing results from UK network models:
- Models need to be run using PyPSA-Eur framework
- Original 'config.yaml' file in PyPSA-Eur repo can be adjusted by using config files from uk-network-models/configs folder
- PyPSA-Eur scripts also need to be modified. Scripts that have been changed have been included in the uk-network-models/scripts folder
- Input parameters for storage technologies by including parameters detailed in the cost-data folder
- Solve UK network by 'snakemake -j2 solve_all_networks'
