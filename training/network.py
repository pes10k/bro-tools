#!/usr/bin/env python
"""
The main entry point for this program.  This is the tool that actually performs
the prediction and implements the LAMSTAR network.
"""
import soms
import sqltypes
import os
import pickle
import numpy
import sys
import random
import argparse
from pprint import pprint

parser = argparse.ArgumentParser()
parser.add_argument('--trains', '-t', type=int, default=1,
                    help="The number of types to run the LAMSTAR network " +
                         "over the data set aside as training data before " +
                         "evaluating the network on the set aside data.")
parser.add_argument('--normalize', '-n', action="store_true")
parser.add_argument('--wrong', '-w', type=float, default=0.0,
                    help="The amount to punish incorrect misses by.")
parser.add_argument('--correct', '-c', type=float, default=0.0,
                    help="The amount to punish correct misses by.")
parser.add_argument('--reward', '-r', type=float, default=(.05 / 20),
                    help="The amount to reward correct predictions.")
parser.add_argument('--max', '-m', default=None,
                    help="The maximum weight any edge can hold.  If not " +
                         "provided, there is no maximum weight.")
parser.add_argument('--rec', type=int, default=0,
                    help="If provided, the maximum number of records " +
                         "to evaluate.  This is really only useful " +
                         "to inspect and debug the network.")
parser.add_argument('--outs', '-o', default=3, type=int,
                    help="The number of output neurons to use.  Only makes " +
                         "sense to use 2 or 3.")
parser.add_argument("--debug", "-d", action="store_true")
args = parser.parse_args()

def debug(msg):
    if args.debug:
        if isinstance(msg, basestring):
            print msg
        else:
            pprint(msg)

# Params to tune
reward = args.reward
punish_wrong = args.wrong
punish_correct = args.correct
max_weight = args.max
normalize = args.normalize
training_rounds = args.trains
random_assignment = True
num_output_neurons = args.outs

features = (
    ("time_from_referrer", soms.TimeFromReferrerSOM()),
    ("time_after_set", soms.TimeAfterSetSOM()),
    ("is_ssl", soms.BoolSOM()),
#    ("graph_size", soms.GraphSizeSOM()),
#    ("is_reachable", soms.BoolSOM()),
#    ("page_rank", soms.PageRankSOM()),
#    ("alexa_rank", soms.AlexiaRankSOM()),
#    ("is_registered", soms.BoolSOM()),
#    ("years_registered", soms.YearsRegisteredSOM()),
#    ("registration_date", soms.DomainRegistrationYearSOM()),
#    ("tag_count", soms.TagCountSOM()),
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
    """Returns the index of the output neuron this record should go to, or
    the index of the correct output neuron given this records label.

    Args:
        row -- a dict describing a labled example, read from the labeled
               training set in the SQLite database.

    Return:
        An integer description of which output neuron should be fired for
        for this example.
    """
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

def adjust_weight(weight):
    """Checks to make sure that the given weight does not exceed the bounds
    described / set by the max_weight runtime option.

    Args:
        weight -- a number, a proposed new weight to be applied in the NN

    Return:
        The same given weight if abs(weight) < max_weight, and otherwise
        sign(weight) * max_weight.  Or, if max_weight is not set, `weight`.
    """
    if not max_weight:
        return weight
    abs_weight = abs(weight)
    new_weight = min(max_weight, abs_weight)
    return new_weight if weight > 0 else (-1 * new_weight)


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

testing_indexes = set()
training_row_index = -1
for round in range(training_rounds):
    record_count = 0
    for row in sqltypes.raw_records():
        record_count += 1

        # If we're using the debug feature of only processing
        # a given number of records, and we've exceeded that number,
        # stop processing now and don't read any further data from the
        # labeled database.
        if args.rec and record_count > args.rec:
            break

        # Optionally print out the entire row for examiniation if we're
        # running in debug mode.
        debug("")
        debug("Record Index: {0}".format(record_count))

        # If this is our first time through the data (ie round 1 of
        # the x number of "training" rounds) then we want to sort
        # each record into one of two sets, either a training set,
        # or a testing set.  To avoid needing to store all records in memory,
        # we don't hold onto the testing / evaluation records for later,
        # but just their index in the set.
        if round == 0:
            training_row_index += 1

            if random_assignment:
                is_testing = random.randint(0, 2) == 0
            else:
                is_testing = training_row_index % 3 == 0

            if is_testing:
                testing_indexes.add(training_row_index)
                debug("Moving record to evaluation set.")
                continue

        # Find the correct, labeled index for this row, given the configured
        # number of output neurons.  Given the default setup of 3 output
        # neurons, this will be in [0, 2]
        label_index = desired_index(row)
        debug("Label: {0}".format(label_index))
        debug(row)

        # Now iterate each of the given features, which are pairs of human
        # readable labels for each feature / subword, and the corresponding
        # SOM object.
        for subword_index in range(len(features)):
            label, som = features[subword_index]
            value = value_for_feature(label, row)

            # If the given training example does not have a value for the
            # given feature, here represented by the None bottom value,
            # then skip over it, and don't use this example for adjusting
            # the weights of the LAMSTAR NN.
            if value is None:
                continue

            # This code, and the SOM implementations I wrote, include
            # functionality for adjusting weights in the SOM, to allow for
            # the full self-mapping functionality of a SOM.  However, since
            # no absolute values are used in this network (only ranges of
            # values) then this functionality is not needed, and thus
            # commented out below.

            # som.add_value(value)
            winning_neuron_index = som.winner(value)
            debug(" - {0} = {1} (index: {2})".format(label, value, winning_neuron_index))
            debug("   - weight: {0} -> {1}".format(
                weights[subword_index][winning_neuron_index][label_index],
                weights[subword_index][winning_neuron_index][label_index] + reward
            ))

            # Once we've determined which index in the subword / input
            # neuron best matches the input value, then strengthen the
            # link between the "winning" neuron in the current subword,
            # and the correct / hand labled output.  If there is a set
            # max weight, make sure that the weight for the neuron doesn't
            # outside the allowed bounds.
            current_weight = weights[subword_index][winning_neuron_index][label_index]
            new_weight = adjust_weight(current_weight + reward)
            weights[subword_index][winning_neuron_index][label_index] = new_weight
            normalizing_counts[subword_index][label_index] += 1

            # Now apply punishments to other neurons in the same subword /
            # input.  Do this by iterating over all the neurons in the
            # current subword. For all links pointing to the desired
            # output neuron (as determined by the labeling) punish the weight
            # by `punish_correct`.
            #
            # For all other neurons in the subword (ie those pointing at
            # other output neurons) punish the weight by `punish_wrong`.
            for s in range(len(weights[subword_index])):
                for v in range(len(weights[subword_index][s])):
                    # No adjustment needed when the correct input neuron
                    # is pointing at the correct output neuron, since that
                    # is already covered above
                    if s == winning_neuron_index:
                        continue
                    current_weight = weights[subword_index][s][v]
                    adjustment = punish_correct if v == label_index else punish_wrong
                    punished_weight = adjust_weight(current_weight - adjustment)
                    weights[subword_index][s][v] = punished_weight

# If we're in debug mode, print out the "learned" weights before normalization.
debug("")
debug("Weights pre-normalization")
debug("---")
debug(weights)

if normalize:
    for input_index in range(len(weights)):
        for neuron_index in range(len(weights[input_index])):
            for j in range(len(weights[input_index][neuron_index])):
                if normalizing_counts[input_index][j] != 0:
                    weights[input_index][neuron_index][j] /= normalizing_counts[input_index][j]
    debug("")
    debug("Weights post normalization")
    debug("---")
    debug(weights)

nn_predictions = []
algo_predictions = []
nothing_predicitons = []
correct_rs = []

results = numpy.zeros((3, 2))
testing_row_index = -1
for row in sqltypes.raw_records():
    testing_row_index += 1

    # If the current row was not set aside to be included in the training set,
    # don't consider it.  `testing_indexes` is a set of the indexes that
    # were selected previously as being for evaluation and not for training.
    if testing_row_index not in testing_indexes:
        continue

    # Now keep track of which output neuron each input neuron votes for,
    # with a simple counter for each output
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
        for j in range(len(index_votes)):
            index_votes[j] += weights[i][winning_index][j]

    debug(index_votes)
    winning_index = list(index_votes).index(max(index_votes))

    # correct for some minor encoding errors
    # if len(winning_indexes) < 2:
    #     continue

#     print row["hash"]
#     print winning_indexes
#     print "Correct: {0}".format(correct)
#     print "Predict: {0}".format(winning_index)
#
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

    results[2][0 if 0 == correct else 1] += 1
    nothing_predicitons.append(abs(0 - correct))

print "Algo:     {0}".format(1 - sum(algo_predictions) / float(len(algo_predictions)))
print "NN:       {0}".format(1 - (sum(nn_predictions) / float(len(nn_predictions))))
print "No Guess: {0}".format(1 - sum(nothing_predicitons) / float(len(nothing_predicitons)))
