# Hookah Topic Modelling using Twitter data

This project attempts to gain insights about the usage of the Hookah, using tweets mentioning the product.

The full notebook can be viewed [here](https://nbviewer.jupyter.org/github/JonAllem/HookahWord2Vec/blob/master/Hookah%20Deep%20Dive.ipynb) using Jupyter's notebook viewer.

## Data Preparation

Hookah as a product was chosen after a brief analysis of tweets belonging to multiple product groups. Hence the data generation/cleaning code in not in one single file.
`Hookah Deep Dive.ipynb` is the main notebook which containing the final results. In order to run this notebook, the data needs to be prepared (normalized, removal of spam, etc.) beforehand. The following sections details each script that needs to be run to prepare the data:

1. `Data/LoadData.linq`: This is the script we used to load the tweets from our Twitter database. It saves the tweets in the `Data/Raw` folder. You can modify the script to use your own database.
2. `Data/clean_data.py`: This script is run on the output of the previous script, and normalizes the tweets. It then stores its results in `Data/Pickles`.
3. `Interproduct Tweet Analysis.ipynb`: This notebook contains some code which partitions the above data into different files - one for each product group we are targeting. These files are also saved in `Data/Pickles`. Since these files are used by our final notebook, we need to run this step even if our original data only contained one product group.
4. `Data/botscore.py`: This script should be run in order to generate botscores for each Twitter user in our dataset. The result of the script is stored in `Data/BotScores`.
5. `Hookah Deep Dive.ipynb`: This is the notebook that contains the actual analysis.