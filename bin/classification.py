from scipy import stats
import pandas as pd

def Entropy(labels:list, base:int=2, condition:bool=False)->float:
    """Calculate the entropy which determind how massive the system is.

    Args:
        labels (list): Classification result of the system.
        base (int, optional): Base of log, better determind with the kinds of labels. Defaults to 2.
        condition (bool, optional): When calculate the information gain, we'll compare the entropy between thr ori sys and the sys which classify by condition A.  Set True to calculate the entropy with condition. Defaults to False.

    Returns:
        float: Entropy.
    """    
    if condition:
        en = 0
        probs = [len(l) for l in labels]
        probs = [p/sum(probs) for p in probs]

        for i, condition_label in enumerate(labels):
            en += probs[i] * Entropy(condition_label, base=base)
        return en
    else:
        probs = pd.Series(labels).value_counts() / len(labels)
        en = stats.entropy(probs, base=base)
        return en

