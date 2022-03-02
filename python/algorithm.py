
# GA0 DEAP_GA

import enum
import json
import numpy as np
import os
import random
import sys
import math
from typing import Iterable, List

from deap import base
from deap import creator
from deap import tools
from deap import algorithms

import eq

# Global variable names we are going to set from the JSON settings file
global_settings = ["num_iter", "num_pop", "sigma", "mate_pb", "mutate_pb"]

# Defaults for Elpy:
mate_pb   = None
mutate_pb = None
num_iter  = None
num_pop   = None

SIM_WORK_TYPE = 1


def message(s):
    print("algorithm.py: " + s, flush=True)


def i2s(i):
    """ Convert individual to string """
    return "[%0.3f,%0.3f]" % (i[0], i[1])


def obj_func(x):
    """ Dummy function for compatibility with DEAP """
    assert(False)


def make_random_params():
    def random_param():
        return random.random() * 4 - 2
    x1 = random_param()
    x2 = random_param()
    return [x1, x2]


def pop_to_json(pop: List[List], param_names: Iterable[str]) -> str:
    res = []
    for individual in pop:
        jmap = {name: individual[i] for i, name in enumerate(param_names)}
        res.append(jmap)

    return json.dumps(res)

def queue_map(obj_func, pop: List[List]):
    """ Note that the obj_func is a dummy
        pops: data that looks like: [[x1,x2],[x1,x2],...]
    """
    if not pop:
        return []
    # eq.OUT_put(create_list_of_lists_string(pops))
    payload = pop_to_json(pop, ('x', 'y'))
    eq_task_id = eq.sumbit_task('test-swift-2', SIM_WORK_TYPE, payload)
    result_status = eq.IN_get(eq_task_id, timeout=2.0)
    if eq.done(result_status):
        # For production this should be more robust,
        # results status can an be an abort or in future a timeout
        return []
    result_str = eq.DB_json_in(eq_task_id)
    # print("RESULT_STR: ", result_str, flush=True)
    result = json.loads(result_str)
    # if max'ing or min'ing and use -9999999 or 99999999
    return [(x,) if not math.isnan(x) else (float(99999999),) for x in result]

    # eq_ids = []
    # for point in pops:
    #     eq_id = eq.DB_submit(eq_type=0, payload=create_json(point))
    #     eq_ids.append(eq_id)
    # eq_ids_bunch = ";".join([ str(x) for x in eq_ids ])
    # eq.OUT_put(0, eq_ids_bunch)
    # eq_ids_bunch = eq.IN_get(eq_type=0)
    # message("IN_get(): tpl: " + str(eq_ids_bunch))
    # if eq.done(eq_ids_bunch):
    #     message("exiting: payload=" + eq_ids_bunch)
    #     # DEAP has no early stopping: Cf. issue #271
    #     exit(1)
    # # Split results string on semicolon
    # tokens = eq_ids_bunch.split(';')
    # # Get the JSON for each eq_id
    # strings = [ eq.DB_json_in(int(token)) for token in tokens ]
    # # Parse each JSON fragment:
    # Js = [ json.loads(s) for s in strings ]
    # # Extract results from JSON and convert to floats in mono-tuples
    # values = [ (float(x["result"]),) for x in Js ]
    # # return [(float(x["result"]),) for x in split_result]
    # print("algorithm: values: " + str(values))
    # return values


def mutate_Gaussian_float(x):
    global sigma
    x += random.gauss(0, sigma)
    return x


# Returns a tuple of one individual
def custom_mutate(individual, indpb):
    old_individual = i2s(individual)
    for i, m in enumerate(individual):
        individual[i] = mutate_Gaussian_float(individual[i])
    print("mutate: %s to: %s" % (old_individual, i2s(individual)))
    return individual,


def read_in_params_csv(csv_file_name):
    import pandas as pd
    return pd.read_csv(csv_file_name)


def cxUniform(ind1, ind2, indpb):
    c1, c2 = tools.cxUniform(ind1, ind2, indpb)
    return (c1, c2)


def run():
    """
    :param num_iter: number of generations
    :param num_pop: size of population
    :param seed: random seed
    :param csv_file_name: csv file name (e.g., "params_for_deap.csv")
    """

    # eq.OUT_put("Settings")
    # settings_filename = eq.IN_get()
    # load_settings(settings_filename)

    # parse settings # num_iter, num_pop, seed,
    eq.init()

    creator.create("FitnessMin", base.Fitness, weights=(-1.0,))
    creator.create("Individual", list, fitness=creator.FitnessMin)
    toolbox = base.Toolbox()
    toolbox.register("attr_float", random.random)
    # toolbox.register("individual", tools.initRepeat, creator.Individual,
    #                 toolbox.attr_float, n=2)
    toolbox.register("individual", tools.initIterate, creator.Individual,
                     make_random_params)

    toolbox.register("population", tools.initRepeat, list, toolbox.individual)
    toolbox.register("evaluate", obj_func)
    toolbox.register("mate", cxUniform, indpb=mate_pb)
    toolbox.register("mutate", custom_mutate, indpb=mutate_pb)
    toolbox.register("select", tools.selTournament, tournsize=int(num_pop/2))
    toolbox.register("map", queue_map)

    pop = toolbox.population(n=num_pop)
    hof = tools.HallOfFame(2)
    stats = tools.Statistics(lambda ind: ind.fitness.values)
    stats.register("avg", np.mean)
    stats.register("std", np.std)
    stats.register("min", np.min)
    stats.register("max", np.max)

    # num_iter-1 generations since the initial population is
    # evaluated once first
    pop, log = algorithms.eaSimple(pop, toolbox, cxpb=0.5, mutpb=mutate_pb,
                                   ngen=num_iter - 1,
                                   stats=stats, halloffame=hof, verbose=True)

    fitnesses = [str(p.fitness.values[0]) for p in pop]

    # eq.OUT_put(eq_type=0, params="EQ_FINAL")
    eq.DB_final(SIM_WORK_TYPE)
    # return the final population
    msg = "{0}\n{1}\n{2}".format(pop_to_json(pop, ('x', 'y')), ';'.join(fitnesses), log)
    # eq.OUT_put(format(msg))
    message(msg)


def load_settings(settings_filename):
    message("reading settings: '%s'" % settings_filename)
    try:
        with open(settings_filename) as fp:
            settings = json.load(fp)
    except IOError:
        message("could not open: '%s'" % settings_filename)
        message("PWD is: '%s'" % os.getcwd())
        sys.exit(1)
    try:
        for s in global_settings:
            message("setting %s=%s" % (s, settings[s]))
            globals()[s] = settings[s]
        random.seed(settings["seed"])
    except KeyError as e:
        message("settings file '%s' does not contain key: %s" %
                (settings_filename, str(e)))
        sys.exit(1)
        # print("num_iter: ", num_iter)
    message("settings loaded.")
