import pickle

import basf2
import basf2.pickle_path as b2pp
from variables import variables as vm


def serialize_module_or_pymodule(module):
    # if module is python module, just use pickle to serialize it to a bytestream
    if module.type() == '' or module.type() == 'PyModule':
        return pickle.dumps(module)
    return {
        'name': module.name(),
        'type': module.type(),
        'flag': module.has_properties(basf2.ModulePropFlags.PARALLELPROCESSINGCERTIFIED),
        'parameters': [{'name': parameter.name, 'values': b2pp.serialize_value(module, parameter)}
                       for parameter in module.available_params()
                       if parameter.setInSteering or module.type() == 'SubEvent'],
        'condition': b2pp.serialize_conditions(module) if module.has_condition() else None}


def serialize_path_with_pymodules(path):
    return {'modules': [serialize_module_or_pymodule(module) for module in path.modules()]}


def get_alias_dict_from_variable_manager():
    """
    Extracts a dictionary with the alias names as keys and their values from the
    internal state of the variable manager and returns it.
    """
    alias_dictionary = {alias_name: vm.getVariable(alias_name).name for alias_name in list(vm.getAliasNames())}
    return alias_dictionary


def write_path_and_aliases_to_file(basf2_path, file_path):
    """
    Serialize basf2 path and variables from variable manage to file

    Variant of ``basf2.pickle_path.write_path_to_file``, only with additional
    serialization of the basf2 variable aliases.  The aliases are extracted from
    the current state of the variable manager singleton and thus have to be
    added in the python/basf2 process before calling this function.

    :param path: Basf2 path object to serialize
    :param file_path: File path to write the serialized pickle object to.
    """
    with open(file_path, 'bw') as pickle_file:
        serialized = serialize_path_with_pymodules(basf2_path)
        serialized["aliases"] = get_alias_dict_from_variable_manager()
        pickle.dump(serialized, pickle_file)
