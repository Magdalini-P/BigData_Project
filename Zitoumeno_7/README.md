7. Για κάθε ένα από τα joins των υλοποιήσεων των Query 3 και Query 4 να αναφερθεί η επιλογή στρατηγικής (BROADCAST, MERGE, SHUFFLE_HASH, SHUFFLE_REPLICATE_NL κλπ.) που
κάνει ο Catalyst Optimizer του Spark με χρήση της εντολής explain ή του Job History Server.
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Υλοποίηση:
Έγινε χρήση της εντολής explain() και το Physical Plan που ακολούθησε ο Catalyst Optimizer πάρθηκε από το k9s.

Τα αρχεία python από τα οποία πήραμε τα αποτελέσματα είναι τα εξής:
- /Zitoumeno_5/query_3_dataframe_api_physplan.py
- /Zitoumeno_6/query_4_dataframe_api_physexplain.py
