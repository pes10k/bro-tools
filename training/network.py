import soms
import sqltypes
import os
import pickle
import numpy
import sys
import random
from pprint import pprint

# Params to tune
reward = .05 / 20
punish_wrong = 0 #.05 / 20
punish_correct = 0 #.05 / 20
weight_max = None
normalize = False
random_assignment = False
debug_index = 1
num_output_neurons = 3

features = (
    ("time_from_referrer", soms.TimeFromReferrerSOM()),
    ("time_after_set", soms.TimeAfterSetSOM()),
    ("is_ssl", soms.BoolSOM()),
    # ("graph_size", soms.GraphSizeSOM()),
    # ("is_reachable", soms.BoolSOM()),
    # ("page_rank", soms.PageRankSOM()),
    # ("alexa_rank", soms.AlexiaRankSOM()),
    # ("is_registered", soms.BoolSOM()),
    # ("years_registered", soms.YearsRegisteredSOM()),
    # ("registration_date", soms.DomainRegistrationYearSOM()),
    # ("tag_count", soms.TagCountSOM()),
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
        weights_for_input.append(numpy.zeros(num_output_neurons))
    weights.append(weights_for_input)

normalizing_counts = numpy.zeros((len(features), num_output_neurons))

def desired_index(row):
    label = row["label"]

    if num_output_neurons == 2:
        if label == "valid":
            return 0
        else:
            return 1
    else:
        if label == "valid":
            return 0
        elif label == "uncertain":
            return 1
        elif label == "stuff":
            return 2

def value_for_feature(label, row):
    if label == "tag_count":
        value = tag_counts[row['tag']]
    else:
        value = row[label]

    if value is None:
        if label == "is_ssl":
            return 0
        return None
    return value

stupid_votes = numpy.zeros((len(features[debug_index][1].weights), num_output_neurons))
testing_indexes = []
training_row_index = -1
record_count = 0
for row in sqltypes.raw_records():
    record_count += 1
    training_row_index += 1

    if random_assignment:
        is_testing = random.randint(0, 2) == 0
    else:
        is_testing = training_row_index % 3 == 0

    if is_testing:
        testing_indexes.append(training_row_index)
        continue

    target_index = desired_index(row)
    for i in range(len(features)):
        label, som = features[i]
        value = value_for_feature(label, row)
        if value is None:
            continue

        som.add_value(value)
        winning_neuron_index = som.winner(value)
        if i == debug_index:
            stupid_votes[winning_neuron_index][target_index] += 1

        weights[i][winning_neuron_index][target_index] += reward
        normalizing_counts[i][target_index] += 1
        if weight_max:
            weights[i][winning_neuron_index][target_index] = min(
                weight_max, weights[i][winning_neuron_index][target_index])

        # Now apply punishments to other neurons
        for s in range(len(weights[i])):
            for v in range(len(weights[i][s])):
                if s != winning_neuron_index:
                    if v == target_index:
                        weights[i][s][v] -= punish_correct
                    else:
                        weights[i][s][v] -= punish_wrong


pprint(stupid_votes)
pprint(normalizing_counts[debug_index])
pprint(weights[debug_index])

print "--"
if normalize:
    for input_index in range(len(weights)):
        for neuron_index in range(len(weights[input_index])):
            for j in range(len(weights[input_index][neuron_index])):
                if normalizing_counts[input_index][j] != 0:
                    weights[input_index][neuron_index][j] /= normalizing_counts[input_index][j]


pprint(weights[debug_index])

# for i in range(len(features)):
#     label, som = features[i]
#     print label
#     pprint(weights[i])

testing_row_index = -1
nn_predictions = []
algo_predictions = []
nothing_predicitons = []
correct_rs = []

print record_count
print len(testing_indexes)
sys.exit()

results = numpy.zeros((3, 2))
for row in sqltypes.raw_records():
    testing_row_index += 1

    # If the current row was not set aside to be included in the training set,
    # don't consider it
    if testing_row_index not in testing_indexes:
        continue

    index_votes = numpy.zeros(num_output_neurons)
    correct = desired_index(row)
    correct_rs.append(correct)

    winning_indexes = []
    for i in range(len(features)):
        label, som = features[i]
        value = value_for_feature(label, row)
        if value is None:
            continue
        winning_index = som.winner(value)
        winning_indexes.append(winning_index)
        print (label, value, winning_index, weights[i][winning_index])
        for j in range(len(index_votes)):
            index_votes[j] += weights[i][winning_index][j]

    winning_index = list(index_votes).index(max(index_votes))

    # correct for some minor encoding errors
    if len(winning_indexes) < 2:
        continue

    print row["hash"]
    print winning_indexes
    print "Correct: {0}".format(correct)
    print "Predict: {0}".format(winning_index)

    results[0][0 if winning_index == correct else 1] += 1


    nn_predictions.append(abs(winning_index - correct))

    time_from_referrer = value_for_feature("time_from_referrer", row)
    time_after_set = value_for_feature("time_after_set", row)
    is_ssl = value_for_feature("is_ssl", row)
    years_registered = value_for_feature("years_registered", row)
    if time_from_referrer < 2 and time_after_set < 2 and not is_ssl:
        algo_result = num_output_neurons - 1
    else:
        algo_result = 0
    results[1][0 if algo_result == correct else 1] += 1

    algo_predictions.append(abs(algo_result - correct))
    print "Algo:    {0}".format(algo_result)

    print "---"

    results[2][0 if 0 == correct else 1] += 1
    nothing_predicitons.append(abs(0 - correct))

# for i in range(len(algo_predictions)):
#     print (correct_rs[i], algo_predictions[i], nn_predictions[i], nothing_predicitons[i])

print "Algo:     {0}".format(1 - sum(algo_predictions) / float(len(algo_predictions)))
print "NN:       {0}".format(1 - (sum(nn_predictions) / float(len(nn_predictions))))
print "No Guess: {0}".format(1 - sum(nothing_predicitons) / float(len(nothing_predicitons)))
pprint(results)
