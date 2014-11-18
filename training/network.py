import soms
import sqltypes
import os
import pickle
import numpy
import random

# Params to tune
reward = .1
punish_wrong = .1
punish_correct = 0

features = (
    ("time_from_referrer", soms.TimeFromReferrerSOM()),
    ("time_after_set", soms.TimeAfterSetSOM()),
    ("graph_size", soms.GraphSizeSOM()),
    ("is_reachable", soms.BoolSOM()),
    ("page_rank", soms.PageRankSOM()),
    ("alexa_rank", soms.AlexiaRankSOM()),
    ("is_registered", soms.BoolSOM()),
    ("years_registered", soms.YearsRegisteredSOM()),
    ("registration_date", soms.DomainRegistrationYearSOM()),
    ("is_ssl", soms.BoolSOM()),
    ("tag_count", soms.TagCountSOM()),
)

h = open(os.path.join("..", "contrib", "tag_counts.pickle"), 'r')
tag_counts = pickle.load(h)


# Structure is
#   input_neuron_index (i) ->
#     neuron_index (k) ->
#       output_neuron_index (j)
weights = []
for i in range(len(features)):
    weights_for_input = []
    for j in range(len(features[i][1].weights)):
        weights_for_input.append(numpy.zeros(3))
    weights.append(weights_for_input)

def desired_index(row):
    label = row["label"]
    if label == "valid":
        return 0
    elif label == "uncertain":
        return 1
    elif label == "stuff":
        return 2

testing_indexes = []
row_index = -1
for row in sqltypes.raw_records():
    row_index += 1

    # Save 1/3rd of the values for evaluation, instead of training
    if random.randint(0, 2) == 0:
        testing_indexes.append(row_index)
        continue

    correct_index = desired_index(row)
    i = 0
    for label, som in features:
        if label == "tag_count":
            value = tag_counts[row['tag']]
        else:
            value = row[label]

        if value is None:
            continue

        som.add_value(value)
        k = som.winner(value)
        for w_k in range(len(weights[i])):
            is_winner = w_k == k
            for w_j in range(len(weights[i][w_k])):
                if is_winner:
                    weights[i][w_k][w_j] += reward
                elif w_j == correct_index:
                    weights[i][w_k][w_j] -= punish_correct
                else:
                    weights[i][w_k][w_j] -= punish_wrong
        i += 1


for label, som in features:
    # Also normalize the weights to be between [0, 1] and to sum to 1
    try:
        prediction = som.winner(pulled_values[label])
        print "label:   {0}".format(label)
        print "predict: {0}".format(prediction)
    except:
        continue
