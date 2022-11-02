
# GA0 DEAP_GA
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

from eqsql import eq
from eqsql import proxies

# Global variable names we are going to set from the JSON settings file
global_settings = ["num_iter", "num_pop", "sigma", "mate_pb", "mutate_pb", "use_proxy"]

# Defaults for Elpy:
mate_pb   = None
mutate_pb = None
num_iter  = None
num_pop   = None
use_proxy = False

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


def pop_to_dict(pop: List[List], param_names: Iterable[str]) -> str:
    res = []
    for individual in pop:
        jmap = {name: individual[i] for i, name in enumerate(param_names)}
        res.append(jmap)
    return res


def queue_map(obj_func, pop: List[List]):
    """ Note that the obj_func is a dummy
        pops: data that looks like: [[x1,x2],[x1,x2],...]
    """
    if not pop:
        return []
    if use_proxy:
        import test_proxy_wf
        # create a proxy for the function we want swift-t to call
        # dump_proxies returns a dict where the keys are the arg names, so
        # ['f'] gets us the proxied function
        func = proxies.dump_proxies(f=test_proxy_wf.task_func)['f']
        proxy_map = proxies.dump_proxies(c=1.0)
        params = pop_to_dict(pop, ('x', 'y'))
        # use the required payload dict names
        payload = json.dumps({'func': func, 'proxies': proxy_map, 'parameters': params})

    else:
        # eq.OUT_put(create_list_of_lists_string(pops))
        payload = pop_to_json(pop, ('x', 'y'))

    status, ft = eq.submit_task('test-swift-2', SIM_WORK_TYPE, payload)
    status, result_str = ft.result(timeout=4.0)
    if status != eq.ResultStatus.SUCCESS:
        print(f'Aborting ME: {result_str}')
        return []
    # print("RESULT_STR: ", result_str, flush=True)
    result = json.loads(result_str)
    # if max'ing or min'ing and use -9999999 or 99999999
    return [(x,) if not math.isnan(x) else (float(99999999),) for x in result]


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

    if use_proxy:
        proxies.init('proxy_test', store_dir='/tmp/proxystore-dump')

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

    # eq.OUT_put(eq_type=0, params="EQ_STOP")
    eq.stop_worker_pool(SIM_WORK_TYPE)
    # return the final population
    msg = "{0}\n{1}\n{2}".format(pop_to_json(pop, ('x', 'y')), ';'.join(fitnesses), log)
    # eq.OUT_put(format(msg))
    message(msg)
    eq.close()


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
