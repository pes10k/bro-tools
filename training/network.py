import soms
import sqltypes
import os
import pickle

features = (
    ("time_from_referrer", soms.TimeFromReferrerSOM(), []),
    ("time_after_set", soms.TimeAfterSetSOM(), []),
    ("graph_size", soms.GraphSizeSOM(), []),
    ("is_reachable", soms.BoolSOM(), []),
    ("page_rank", soms.PageRankSOM(), []),
    ("alexa_rank", soms.AlexiaRankSOM(), []),
    ("is_registered", soms.BoolSOM(), []),
    ("years_registered", soms.YearsRegisteredSOM(), []),
    ("registration_date", soms.DomainRegistrationYearSOM(), []),
    ("is_ssl", soms.BoolSOM(), []),
    ("tag_count", soms.TagCountSOM(), []),
)

records = {}

h = open(os.path.join("..", "contrib", "tag_counts.pickle"), 'r')
tag_counts = pickle.load(h)

for row in sqltypes.raw_records():
    for label, SOM, records in features:
        if label == "tag_count":
            value = tag_counts[row['tag']]
        else:
            value = row[label]

        if value is None:
            continue

        records.append(SOM.vector_for_value(value))

for label, SOM, records in features:
    SOM.train_random(records, len(records) * 3)
