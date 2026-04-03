"""
To expand our research, we looked for a labeled dataset 
that could provide the right amount of data well focused 
on the topic of stock market and finance. The “financial_
phrasebank” dataset which was developed and used in the 
work of Malo et al. [21] met the needs of our research. 
This dataset included 5000 labeled sentences from finan
cial news articles about Finnish Banks. The sentiment 
behind each sentence was identified by people with suf
f
icient knowledge of the financial world. These sentences 
were appropriate for our sentiment analysis research, as 
they included news on corporate finances as well as news 
unrelated to corporate internal affairs, focusing on external 
sentiments and assessments. Using this data we were able 
to expand and strengthen our research, especially when 
it was used in combination with the datasets mentioned 
above.    
"""
# qui importo il dataset da hugging face, che è un dataset di frasi etichettate con sentimenti, che viene citato nell'alrticolo
from datasets import load_dataset

# Load the highest-agreement configuration
ds = load_dataset("financial_phrasebank", "sentences_allagree", trust_remote_code=True)

print(ds)
print(ds["train"][0])
