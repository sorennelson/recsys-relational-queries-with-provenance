from __future__ import absolute_import
from __future__ import annotations
from __future__ import division
from __future__ import print_function

import csv
import logging
from enum import Enum
from operator import delitem
from turtle import left, right
from typing import List, Tuple
import uuid

import argparse
import ray
import pandas as pd
import copy

# Note (john): Make sure you use Python's logger to log
#              information about your program
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

BATCH_SIZE=1000

# Generates unique operator IDs
def _generate_uuid():
    return uuid.uuid4()

# Partition strategy enum
class PartitionStrategy(Enum):
    RR = "Round_Robin"
    HASH = "Hash_Based"

# Custom tuple class with optional metadata
class ATuple:
    """Custom tuple.

    Attributes:
        tuple (Tuple): The actual tuple.
        metadata (string): The tuple metadata (e.g. provenance annotations).
        operator (Operator): A handle to the operator that produced the tuple.
    """
    def __init__(self, tuple, metadata=None, operator=None):
        self.tuple = tuple
        self.metadata = metadata
        self.operator = operator

    # Returns the lineage of self
    def lineage(self, is_start=False) -> List[ATuple]:
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        if is_start:
            return self.operator.lineage([self], is_start)
        else:
            return self.operator.lineage([self])

    # Returns the Where-provenance of the attribute at index 'att_index' of self
    def where(self, att_index, is_start) -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        # return self.operator.where(att_index, [self])
        if is_start:
            return self.operator.where(att_index, [self], is_start)
        else:
            return self.operator.where(att_index, [self])

    # Returns the How-provenance of self
    def how(self) -> string:
        # YOUR CODE HERE (ONLY FOR TASK 3 IN ASSIGNMENT 2)
        return self.metadata

    # Returns the input tuples with responsibility \rho >= 0.5 (if any)
    def responsible_inputs(self, orderby, scans) -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 4 IN ASSIGNMENT 2)

        # Query specific for the time being, assumes avg, order-by, limit 1
        assert self.metadata[:5] == 'AVG( '
        
        items = self.__parse_metadata()
        responsible_inputs = []

        checked = set()
        for item in items:
            _, tups = item
            for tup in tups:
                if tup in checked:
                    continue
                checked.add(tup)
                responsibility = 0

                # Check if counterfactual - responsibility 1
                new_avgs, counterfactual, avg_items = self.__is_counterfactual(
                                                                orderby, 
                                                                tup, 
                                                                orderby.sorted_tups)
                if counterfactual:
                    responsibility = 1
                    responsible_inputs.append((tup, responsibility))
                    break

                # Check contingency set size 1 counterfactual - responsibility 0.5
                contingency_checked = set([tup])

                for contingency_item in items:
                    _, contingency_tups = contingency_item
                    for contingency_tup in contingency_tups:
                        if contingency_tup in contingency_checked:
                            continue
                        contingency_checked.add(contingency_tup)

                        _, actual_cause, _ = self.__is_counterfactual(
                                                        orderby, 
                                                        contingency_tup, 
                                                        new_avgs,
                                                        avg_items)
                        if actual_cause:
                            responsibility = 0.5
                            responsible_inputs.append((tup, responsibility))
                            break
                    
                    if responsibility == 0.5:
                        break

        # Grab input tuples for each scan so we only need to scan each again once
        input_tuples = dict()
        for scan in scans:
            ids = [inp[0] for inp in responsible_inputs if inp[0][0] == scan.fid]
            input_tuples.update(scan.find_input_tuples(ids))

        # Change ids in responsible_inputs to input tuple
        for i in range(len(responsible_inputs)):
            id = responsible_inputs[i][0]
            responsible_inputs[i] = (input_tuples[id], responsibility)
        
        return responsible_inputs

    def __is_counterfactual(self, orderby, tup, curr_set, all_avg_items=[]):
        avgs = [*curr_set]
        new_all_avg_items = []

        for i, avg in enumerate(avgs):
            # Either use metadata or if already adjusted then use items from previous round
            avg_items = all_avg_items[i] if all_avg_items else avg.__parse_metadata()
            new_avg_items, avg_item_vals = [], []
            for avg_item in avg_items:
                avg_tup_val, avg_tups = avg_item
                remove = False
                # 0 out all tups == tup
                for avg_tup in avg_tups:
                    if avg_tup == tup:
                        remove = True
                        break
                if not remove:
                    avg_item_vals.append(int(avg_tup_val))
                    new_avg_items.append((avg_tup_val, avg_tups))
            # Recompute avg
            new_avg = sum(avg_item_vals)/len(avg_item_vals) if avg_item_vals else 0
            avg.tuple = (avg.tuple[0], new_avg)
            new_all_avg_items.append(new_avg_items)
        
        # Re-sort
        new_sorting = sorted(avgs, key=orderby.comparator, reverse=not orderby.ASC)
        # If sorting order changed then the tuple is a counteractual for curr_set
        counterfactual = new_sorting[0].metadata != orderby.sorted_tups[0].metadata
        return avgs, counterfactual, new_all_avg_items

    def __parse_metadata(self):
        metadata = self.metadata[5:-2]
        items = metadata.split(', ')
        return [self.__parse_metadata_item(i) for i in items]
        
    def __parse_metadata_item(self, item):
        val = item[1:-1].split('@')[-1]
        rest = item[1:-1].split('@')[0]
        if '*' in rest:
            tups = [rest.split('*')[0], 
                    rest.split('*')[1]]
        else:
            tups = [rest]
        return (val, tups)


    # Print only the tuple
    def __repr__(self) -> str:
        return str(self.tuple)
        

# Data operator
class Operator:
    """Data operator (parent class).

    Attributes:
        id (string): Unique operator ID.
        name (string): Operator name.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    def __init__(self,
                 id=None,
                 name=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        self.id = _generate_uuid() if id is None else id
        self.name = "Undefined" if name is None else name
        self.track_prov = track_prov
        self.propagate_prov = propagate_prov
        self.pull = pull
        self.partition_strategy = partition_strategy
        logger.debug("Created {} operator with id {}".format(self.name,
                                                             self.id))

    # NOTE (john): Must be implemented by the subclasses
    def get_next(self) -> List[ATuple]:
        logger.error("Method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def lineage(self, tuples: List[ATuple]) -> List[List[ATuple]]:
        logger.error("Lineage method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def where(self, att_index: int, tuples: List[ATuple]) -> List[List[Tuple]]:
        logger.error("Where-provenance method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def apply(self, tuples: List[ATuple]) -> bool:
        logger.error("Apply method is not implemented!")

# Scan operator
class Scan(Operator):
    """Scan operator.

    Attributes:
        filepath (string): The path to the input file.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        filter (function): An optional user-defined filter.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes scan operator
    def __init__(self,
                 filepath,
                 outputs : List[Operator],
                 filter=None,
                 table_name=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        super(Scan, self).__init__(name="Scan",
                                   track_prov=track_prov,
                                   propagate_prov=propagate_prov,
                                   pull=pull,
                                   partition_strategy=partition_strategy)
        # YOUR CODE HERE
        # pass
        self.fp = filepath
        self.outputs = outputs
        self.filter = filter
        self.fname = self.fp.split('/')[-1]
        self.fid = table_name[0].lower() if table_name else self.fname[0].lower()
        self.lid = 1

        self.batch_size = BATCH_SIZE
        self.finished = False
        self.data = pd.read_csv(self.fp, chunksize=self.batch_size, sep=' ')
        self.join_output = None

    def set_join_output_side(self, is_right):
        self.join_output = is_right

    # Returns next batch of tuples in given file (or None if file exhausted)
    def get_next(self):
        # YOUR CODE HERE
        if self.finished: 
            return None
        
        # Get next batch of data
        try:
            df_batch = self.data.get_chunk()
        except:
            self.finished = True
            return None

        # EOF
        if len(df_batch) == 0:
            self.finished = True
            return None

        # Create and return tuples
        tuples = self._pd_to_tuples(df_batch)
        return tuples

    def _pd_to_tuples(self, df_batch):
        # [:-1] because of NAN column
        tuples = []
        for i in list(df_batch.itertuples(index=False, name=None)):
            tuples.append(
                ATuple(i[:-1], 
                    '{}{}'.format(self.fid, self.lid) if self.propagate_prov else None, 
                    self)
                )
            self.lid += 1
        return tuples

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        if not self.pull:
            tuple_lineage = []
            for output in self.outputs:
                if self.join_output is None:
                    tuple_lineage += output.lineage(tuples)
                else:
                    tuple_lineage += output.lineage(tuples, self.join_output)
        else:
            tuple_lineage = tuples

        return tuple_lineage


    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)

        if not self.pull:
            tuple_lineage = []
            for output in self.outputs:
                if self.join_output is None:
                    tuple_lineage += output.where(att_index, tuples)
                else:
                    tuple_lineage += output.where(att_index, tuples, self.join_output)
            
            if not tuple_lineage:
                return []

            # Update our att_index from upstream
            att_index = tuple_lineage[0].tuple[1]

            # Remove att_index from tuples
            tuples = []
            for tup in tuple_lineage:
                tuples.append(
                    ATuple(tup.tuple[0], tup.metadata, tup.operator)
                )
        
        # Scan input to grab line_number
        self.data = pd.read_csv(self.fp, chunksize=self.batch_size, sep=' ')
        global_idx, input_tups = 1, []
        tuple_data = [tup.tuple for tup in tuples]
        while True:
            try:
                df_batch = self.data.get_chunk()                    
                for tup in df_batch.itertuples(index=False, name=None):
                    if tup[:-1] in tuple_data:
                        file_name = self.fname
                        data = (file_name, global_idx, tup[:-1], tup[att_index])
                        input_tups.append(
                            ATuple(data, None, self)
                        )
                    global_idx += 1
            except:
                break

            tuple_lineage = input_tups

        return tuple_lineage

    # Scans input to find tuples corresponding to tuple ids (e.g. r5)
    def find_input_tuples(self, ids):
        self.data = pd.read_csv(self.fp, chunksize=self.batch_size, sep=' ')
        global_idx, input_tups = 1, dict()
        lines = [int(id[1:]) for id in ids]
        while True:
            try:
                df_batch = self.data.get_chunk()                    
                for tup in df_batch.itertuples(index=False, name=None):
                    if global_idx in lines:
                        tid = '{}{}'.format(self.fid, global_idx)
                        input_tups[tid] = ATuple(tup[:-1], None, self)
                    global_idx += 1
            except:
                break

        return input_tups


    # Starts the process of reading tuples (only for push-based evaluation)
    def start(self):
        while True:
            # Get next batch of data
            try:
                df_batch = self.data.get_chunk()

            # EOF (TODO: ideally - maybe worth cleaning this up later)
            except:
                break

            # EOF
            if len(df_batch) == 0:
                break

            # Create and send tuples
            tuples = self._pd_to_tuples(df_batch)
            for output in self.outputs:
                if self.join_output is None:
                    output.apply(tuples)
                else:
                    output.apply(tuples, self.join_output)

        # EOF
        for output in self.outputs:
            if self.join_output is None:
                output.apply(None)
            else:
                output.apply(None, self.join_output)
                
# Equi-join operator
class Join(Operator):
    """Equi-join operator.

    Attributes:
        left_inputs (List): A list of handles to the instances of the operator
        that produces the left input.
        right_inputs (List):A list of handles to the instances of the operator
        that produces the right input.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        left_join_attribute (int): The index of the left join attribute.
        right_join_attribute (int): The index of the right join attribute.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes join operator
    def __init__(self,
                 left_inputs : List[Operator],
                 right_inputs : List[Operator],
                 outputs : List[Operator],
                 left_join_attribute,
                 right_join_attribute,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        super(Join, self).__init__(name="Join",
                                   track_prov=track_prov,
                                   propagate_prov=propagate_prov,
                                   pull=pull,
                                   partition_strategy=partition_strategy)
        # YOUR CODE HERE
        self.left_inputs = left_inputs
        self.right_inputs = right_inputs
        self.outputs = outputs
        self.left_join_attribute = left_join_attribute
        self.right_join_attribute = right_join_attribute
        self.left_tup_size = 0

        self.batch_size = BATCH_SIZE

        if self.pull:
            self.left_dict = dict()
            self.left_hashed = False

        else:
            self.left_dict = dict()
            self.right_dict = dict()
            self.left_finished = False
            self.right_finished = False
            

    # Returns next batch of joined tuples (or None if done)
    def get_next(self):
        # YOUR CODE HERE

        if not self.left_hashed:
            # Block while hashing all of left table
            self.__hash_full_left()
            # Unblock after left table is in memory
            self.left_hashed = True

        # Probe with right
        right_tups = self.__pull_input_tups(self.right_inputs)
        # End of inputs
        if right_tups is None:
            return None
        joined_tups, _ = self.__probe(right_tups, 
                                    self.right_join_attribute,
                                    self.left_dict,
                                    is_right=True)
        return joined_tups 

    # Hash all of left table and store in memory
    def __hash_full_left(self):
        left_tups = self.__pull_input_tups(self.left_inputs)
        while left_tups is not None:
            _ = self.__hash(left_tups, self.left_dict, is_right=False)
            left_tups = self.__pull_input_tups(self.left_inputs)

    # Pull batch of input tuples
    def __pull_input_tups(self, inputs):
        tups = []
        finished = True
        for inp in inputs:
            next = inp.get_next()
            if next is not None:
                tups.extend(next)
                finished = False

        # Return None if done
        return None if finished else tups

    # Hash a batch of tuples into the given dictionary
    #   is_right: Hashing with right
    def __hash(self, tuples, dict, is_right):
        joined_tups = []
        for hashed_tup in tuples:
            join_attr = self.right_join_attribute if is_right else self.left_join_attribute
            attr = hashed_tup.tuple[join_attr]

            if attr in dict:
                dict[attr]['pushed'].append(hashed_tup)

                if 'probed' in dict[attr]:
                    # Join on push-based any previous succesful probes
                    for probe_tup in dict[attr]['probed']:
                        joined_tup = self.__join(hashed_tup, probe_tup, is_right)
                        joined_tups.append(joined_tup)

            else:
                dict[attr] = {'pushed': [hashed_tup]}

        return joined_tups

    # Probe a dict with the given tuples on the given attribute. 
    #   is_right: Probing with right
    def __probe(self, tups, join_attr, dict, is_right):
        joined_tups, non_joined = [], []

        for probe_tup in tups:
            attr = probe_tup.tuple[join_attr]
            # Probe opposite dict
            if attr in dict:

                # If push-based we need to store joined probe tuples 
                #   (so we can join with future tuples)
                if not self.pull:
                    if 'probed' in dict[attr]:
                        dict[attr]['probed'].append(probe_tup)
                    else:
                        dict[attr]['probed'] = [probe_tup]

                for hashed_tup in dict[attr]['pushed']:
                    joined_tup = self.__join(hashed_tup, probe_tup, is_right)
                    joined_tups.append(joined_tup)
            else:
                # Store non_joined probing tups for push-based
                #   (so we can push to own dict for future tuples)
                non_joined.append(probe_tup)
        return joined_tups, non_joined

    # Join a left and right tuple
    #   is_right: Probing with right
    def __join(self, hashed_tup, probe_tup, is_right):
        metadata = ''
        # Remove join attribute from right tuple
        if is_right:
            right_data = [probe_tup.tuple[i] for i in range(len(probe_tup.tuple)) if i != self.right_join_attribute]
            joined_data = tuple([x for x in hashed_tup.tuple] + right_data)

            if self.track_prov:
                self.left_tup_size = len(hashed_tup.tuple)
            if self.propagate_prov:
                metadata = '{}*{}'.format(hashed_tup.metadata, probe_tup.metadata)

        else:
            right_data = [hashed_tup.tuple[i] for i in range(len(hashed_tup.tuple)) if i != self.right_join_attribute]
            joined_data = tuple([x for x in probe_tup.tuple] + right_data)

            if self.track_prov:
                self.left_tup_size = len(probe_tup.tuple)
            if self.propagate_prov:
                metadata = '{}*{}'.format(probe_tup.metadata, hashed_tup.metadata)
            

        return ATuple(joined_data, metadata, self)

    # Returns the lineage of the given tuples
    def lineage(self, tuples, is_right=False):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        
        tuple_lineage = []
        if self.pull:
            l_inp_tuples, r_inp_tuples = self.__map_outputs_to_inputs(tuples)
            for inp in self.left_inputs:
                tuple_lineage += inp.lineage(l_inp_tuples)

            for inp in self.right_inputs:
                tuple_lineage += inp.lineage(r_inp_tuples)
        else:
            for output in self.outputs:
                tuple_lineage += output.lineage(tuples)
            l_inp_tuples, r_inp_tuples = self.__map_outputs_to_inputs(tuple_lineage)
            tuple_lineage = r_inp_tuples if is_right else l_inp_tuples
                
        return tuple_lineage

    def __map_outputs_to_inputs(self, tuples):
        r_inp_tuples, l_inp_tuples = [], []
        for tup in tuples:
            l_inp_tuples += [ATuple(tup.tuple[:self.left_tup_size])]

            # Insert join attribute back into right tuple
            right_data = list(
                            tup.tuple[self.left_tup_size: self.left_tup_size+self.right_join_attribute]) + \
                            [tup.tuple[self.left_join_attribute]] + \
                            list(tup.tuple[self.left_tup_size+self.right_join_attribute:]
                            )
            r_inp_tuples += [ATuple(tuple(right_data))]
        return l_inp_tuples, r_inp_tuples

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples, is_right=False):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)

        tuple_lineage = []
        if self.pull:
            l_inp_tuples, r_inp_tuples = self.__map_outputs_to_inputs(tuples)

            # L/R
            if att_index < self.left_tup_size:
                for inp in self.left_inputs:
                    tuple_lineage += inp.where(att_index, l_inp_tuples)
            else:
                for inp in self.right_inputs:
                    tuple_lineage += inp.where(att_index + 1 - self.left_tup_size, r_inp_tuples)
        else:
            for output in self.outputs:
                tuple_lineage += output.where(att_index, tuples)

            # Remove att_index from tuple for mapping
            output_lineage = []
            for tup in tuple_lineage:
                next_att_index = tup.tuple[1]
                output_lineage.append(
                    ATuple(tup.tuple[0], tup.metadata, tup.operator)
                )

            l_inp_tuples, r_inp_tuples = self.__map_outputs_to_inputs(output_lineage)

            # Update att_index, return correct lineage to caller
            if next_att_index >= self.left_tup_size:
                next_att_index = next_att_index + 1 - self.left_tup_size
                input_lineage = r_inp_tuples if is_right else []
            else:
                input_lineage = l_inp_tuples if not is_right else []

            # Add att_index to tuple after mapping
            tuple_lineage = []
            for tup in input_lineage:
                tuple_lineage.append(
                    ATuple((tup.tuple, next_att_index), 
                            tup.metadata, 
                            tup.operator)
                )
                
        return tuple_lineage

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple], is_right):
        # TODO: Extend to all ops (Only select/Scan right now)
        # Keep a left and right hashed dictionary. When we recieve 
        # a new batch for e.g. left we probe right dict then hash 
        # any tuples that couldn't join on the probe to left dict.
        # Any succesful probes are stored on right dict to be joined
        # with future hashes of the right table.

        if tuples is None:
            # Check opposite l/r_finished - if both are done then end
            done = False
            if is_right:
                self.right_finished = True
                done = self.left_finished
            else:
                self.left_finished = True
                done = self.right_finished
            
            if done:
                # Send to output ops
                for output in self.outputs:
                    output.apply(None)
            return
        
        if not is_right:
            join_attr = self.left_join_attribute
            dict = self.left_dict
            opp_dict = self.right_dict
        else:
            join_attr = self.right_join_attribute
            dict = self.right_dict
            opp_dict = self.left_dict

        # Probe opposite dict
        joined_tups, non_joined_tups = self.__probe(tuples, join_attr, opp_dict, is_right)

        # Push remaining tuples to this dict
        joined_tups.extend(
            self.__hash(non_joined_tups, dict, is_right))

        # Push joined tuples to next ops
        for output in self.outputs:
            output.apply(joined_tups)

# Project operator
class Project(Operator):
    """Project operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        fields_to_keep (List(int)): A list of attribute indices to keep.
        If empty, the project operator behaves like an identity map, i.e., it
        produces and output that is identical to its input.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes project operator
    def __init__(self,
                 inputs : List[Operator],
                 outputs : List[None],
                 fields_to_keep=[],
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        super(Project, self).__init__(name="Project",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov,
                                      pull=pull,
                                      partition_strategy=partition_strategy)
        # YOUR CODE HERE
        self.inputs = inputs
        self.outputs = outputs
        self.fields_to_keep = fields_to_keep
        self.output_to_inputs = dict()

    # Return next batch of projected tuples (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        # Get all tups from input ops
        tups = []
        finished = True
        for inp in self.inputs:
            next = inp.get_next()
            if next is not None:
                tups.extend(next)
                finished = False

        if finished:
            return None

        # Project
        self.__project(tups)

        return tups

    # Runs a projection on a list of tuples, in place
    def __project(self, tups):
        for i in range(len(tups)):
            data = tups[i].tuple
            # Project
            new_data = tuple(data[j] for j in self.fields_to_keep)
            # Update ATuple in list
            projected = ATuple(new_data, tups[i].metadata, self)

            if self.track_prov:
                # Store input/output mapping
                if new_data in self.output_to_inputs:
                    self.output_to_inputs[new_data].append(tups[i])
                else:
                    self.output_to_inputs[new_data] = [tups[i]]

            tups[i] = projected

        
    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        
        tuple_lineage = []
        if self.pull:
            inp_tuples = self.__map_outputs_to_inputs(tuples)
            
            for inp in self.inputs:
                tuple_lineage += inp.lineage(inp_tuples)
        else:   
            for output in self.outputs:
                tuple_lineage += output.lineage(tuples)

            tuple_lineage = self.__map_outputs_to_inputs(tuple_lineage)
        
        return tuple_lineage

    def __map_outputs_to_inputs(self, tuples):
        inp_tuples = []
        for tup in tuples:
            inp_tuples += self.output_to_inputs[tup.tuple]
        return inp_tuples
            

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)

        tuple_lineage = []
        if self.pull:
            next_att_index = self.fields_to_keep[att_index]
            inp_tuples = self.__map_outputs_to_inputs(tuples)
            
            for inp in self.inputs:
                tuple_lineage += inp.where(next_att_index, inp_tuples)
        else:   
            for output in self.outputs:
                tuple_lineage += output.where(att_index, tuples)

            # Remove att_index from tuple for mapping
            output_lineage = []
            for tup in tuple_lineage:
                next_att_index = self.fields_to_keep[tup.tuple[1]]
                output_lineage.append(
                    ATuple(tup.tuple[0], tup.metadata, tup.operator)
                )
                
            input_lineage = self.__map_outputs_to_inputs(output_lineage)

            # Add att_index to tuple after mapping
            tuple_lineage = []
            for tup in input_lineage:
                tuple_lineage.append(
                    ATuple((tup.tuple, next_att_index), 
                            tup.metadata, 
                            tup.operator)
                )

        return tuple_lineage

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):

        # Project
        if tuples is not None:
            self.__project(tuples)

        # Send to output ops
        for output in self.outputs:
            output.apply(tuples)
        
# Group-by operator
class GroupBy(Operator):
    """Group-by operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        key (int): The index of the key to group tuples.
        value (int): The index of the attribute we want to aggregate.
        agg_fun (function): The aggregation function.
        agg_fun_name (string): The name of aggregation function (e.g. 'AVG')
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes average operator
    def __init__(self,
                 inputs : List[Operator],
                 outputs : List[Operator],
                 key,
                 value,
                 agg_fun,
                 agg_fun_name=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        super(GroupBy, self).__init__(name="GroupBy",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov,
                                      pull=pull,
                                      partition_strategy=partition_strategy)
        # YOUR CODE HERE
        self.inputs = inputs
        self.outputs = outputs
        self.key = key
        self.value = value
        self.agg_fun = agg_fun
        self.groups = dict()
        self.grouped = False
        
        self.batch_size = BATCH_SIZE
        
        self.output_to_inputs = dict()
        self.agg_fun_name = agg_fun_name
        if self.propagate_prov:
            self.group_metadata = dict()

    # Returns aggregated value per distinct key in the input (or None if done)
    def get_next(self):
        # YOUR CODE HERE

        # Block until tuples are all grouped
        while not self.grouped:
            tups = self.__pull_input_tups()
            if tups is None:
                self.grouped = True
            else:
                self.__group_stats(tups)
        
        grouped_tuples = self.__create_tuples_from_stats()

        return grouped_tuples

    # Compile stats per group for batch of input tuples
    def __group_stats(self, tups):
        for i in range(len(tups)):
            data = tups[i].tuple
            key = data[self.key] if self.key is not None else 'ALL'
            if self.propagate_prov:
                metadata = '{}@{}'.format(tups[i].metadata, data[self.value])

            if key in self.groups:
                self.groups[key].append(data[self.value])
                if self.track_prov:
                    self.output_to_inputs[key].append(tups[i])
                if self.propagate_prov:
                    self.group_metadata[key].append(metadata)
            else:
                self.groups[key] = [data[self.value]]
                if self.track_prov:
                    self.output_to_inputs[key] = [tups[i]]
                if self.propagate_prov:
                    
                    self.group_metadata[key] = [metadata]

    # Create our new output tuples from group stats
    def __create_tuples_from_stats(self):
        # No groups left
        if len(self.groups) == 0:
            return None
        
        tups, groups_to_remove = [], []
        for i, (group, stat) in enumerate(self.groups.items()):
            metadata = None
            if self.propagate_prov:
                tup_meta = '({})'.format('), ('.join(self.group_metadata[group]))
                metadata = '{}( {} )'.format(self.agg_fun_name, tup_meta)

            tups.append(
                ATuple((group, self.agg_fun(stat),), 
                        metadata, 
                        self)
                )

            groups_to_remove.append(group)
            if i == self.batch_size - 1: break

        # Remove groups from dict
        for i in range(len(groups_to_remove)):
            del self.groups[groups_to_remove[i]]
            
        return tups

    # Pull batch of input tuples
    def __pull_input_tups(self):
        tups = []
        finished = True
        for inp in self.inputs:
            next = inp.get_next()
            if next is not None:
                tups.extend(next)
                finished = False

        # Return None if done
        return None if finished else tups            

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        
        tuple_lineage = []
        if self.pull:
            inp_tuples = self.__map_outputs_to_inputs(tuples)
            for inp in self.inputs:
                tuple_lineage += inp.lineage(inp_tuples)
        else:
            for output in self.outputs:
                tuple_lineage += output.lineage(tuples)

            tuple_lineage = self.__map_outputs_to_inputs(tuple_lineage)
        
        return tuple_lineage

    def __map_outputs_to_inputs(self, tuples):
        inp_tuples = []
        for tup in tuples:
            inp_tuples += self.output_to_inputs[tup.tuple[0]]
        return inp_tuples
        

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)

        tuple_lineage = []
        if self.pull:
            next_att_index = self.key if att_index == 0 else self.value
            inp_tuples = self.__map_outputs_to_inputs(tuples)
            for inp in self.inputs:
                tuple_lineage += inp.where(next_att_index, inp_tuples)
        else:
            for output in self.outputs:
                tuple_lineage += output.where(att_index, tuples)

            # Remove att_index from tuple for mapping
            output_lineage = []
            for tup in tuple_lineage:
                next_att_index = self.key if tup.tuple[1] == 0 else self.value
                output_lineage.append(
                    ATuple(tup.tuple[0], tup.metadata, tup.operator)
                )

            input_lineage = self.__map_outputs_to_inputs(output_lineage)

            # Add att_index to tuple after mapping
            tuple_lineage = []
            for tup in input_lineage:
                tuple_lineage.append(
                    ATuple((tup.tuple, next_att_index), 
                            tup.metadata, 
                            tup.operator)
                )
        
        return tuple_lineage

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        # Block until we have grouped all stats
        if not self.grouped:
            if tuples is not None:
                self.__group_stats(tuples)
            else:
                self.grouped = True

        if self.grouped:
            # Output our stats in batches
            grouped_tuples = self.__create_tuples_from_stats()
            while grouped_tuples is not None:
                for output in self.outputs:
                    output.apply(grouped_tuples)

                grouped_tuples = self.__create_tuples_from_stats()

            for output in self.outputs:
                output.apply(None)

# Custom histogram operator
class Histogram(Operator):
    """Histogram operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        key (int): The index of the key to group tuples. The operator outputs
        the total number of tuples per distinct key.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes histogram operator
    def __init__(self,
                 inputs : List[Operator],
                 outputs : List[Operator],
                 key=0,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        super(Histogram, self).__init__(name="Histogram",
                                        track_prov=track_prov,
                                        propagate_prov=propagate_prov,
                                        pull=pull,
                                        partition_strategy=partition_strategy)
        # YOUR CODE HERE
        self.inputs = inputs
        self.outputs = outputs
        self.key = key
        self.buckets = dict() # Stores {bucket: count}
        self.grouped = False
        
        self.batch_size = BATCH_SIZE

        self.output_to_inputs = dict()

    # Returns histogram (or None if done)
    def get_next(self):
        # YOUR CODE HERE

        # Block until tuples are all in buckets
        while not self.grouped:
            tups = self.__pull_input_tups()
            if tups is None:
                self.grouped = True
            else:
                self.__group_stats(tups)
    
        grouped_tuples = self.__create_tuples_from_stats()
        return grouped_tuples

    # Compile stats per group for batch of input tuples
    def __group_stats(self, tups):
        for i in range(len(tups)):
            key = tups[i].tuple[self.key]

            if key in self.buckets:
                self.buckets[key] += 1
                if self.track_prov:
                    self.output_to_inputs[key].append(tups[i])
            else:
                self.buckets[key] = 1
                if self.track_prov:
                    self.output_to_inputs[key] = [tups[i]]

    # Create our new output tuples from group stats
    def __create_tuples_from_stats(self):
        # No buckets left
        if self.buckets is None:
            return None

        tups = []
        for bucket, stat in self.buckets.items():
            tups.append(ATuple((bucket, stat), None, self))
        self.buckets = None
            
        return tups

    # Pull batch of input tuples
    def __pull_input_tups(self):
        tups = []
        finished = True
        for inp in self.inputs:
            next = inp.get_next()
            if next is not None:
                tups.extend(next)
                finished = False

        # Return None if done
        return None if finished else tups      

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        
        tuple_lineage = []
        if self.pull:
            inp_tuples = self.__map_outputs_to_inputs(tuples)
            for inp in self.inputs:
                tuple_lineage += inp.lineage(inp_tuples)
        else:
            for output in self.outputs:
                tuple_lineage += output.lineage(tuples)

            tuple_lineage = self.__map_outputs_to_inputs(tuple_lineage)
            
        return tuple_lineage

    def __map_outputs_to_inputs(self, tuples):
        inp_tuples = []
        for tup in tuples:
            inp_tuples += self.output_to_inputs[tup.tuple[0]]
        return inp_tuples

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        # Block until grouped all stats
        if not self.grouped:
            if tuples is not None:
                self.__group_stats(tuples)
            else:
                self.grouped = True

        if self.grouped:
            # Output our stats in batches
            grouped_tuples = self.__create_tuples_from_stats()
            while grouped_tuples is not None:
                for output in self.outputs:
                    output.apply(grouped_tuples)

                grouped_tuples = self.__create_tuples_from_stats()

            for output in self.outputs:
                output.apply(None)

# Order by operator
class OrderBy(Operator):
    """OrderBy operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        comparator (function): The user-defined comparator used for sorting the
        input tuples.
        ASC (bool): True if sorting in ascending order, False otherwise.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes order-by operator
    def __init__(self,
                 inputs : List[Operator],
                 outputs : List[Operator],
                 comparator,
                 ASC=True,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        super(OrderBy, self).__init__(name="OrderBy",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov,
                                      pull=pull,
                                      partition_strategy=partition_strategy)
        # YOUR CODE HERE
        self.inputs = inputs
        self.outputs = outputs
        self.comparator = comparator
        self.ASC = ASC
        self.tups = []
        self.loaded = False
        self.batch_size = BATCH_SIZE

    # Returns the sorted input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        
        # Block until all tups are loaded and sorted
        if not self.loaded:
            self.__pull_and_sort()
            self.loaded = True
        
        # Return None if done
        if not self.tups:
            return None

        # Output first batch_size items
        end_ind = min(self.batch_size, len(self.tups))
        output_tups = self.tups[:end_ind]
        # Remove output tups from list
        self.tups = self.tups[end_ind:]
        return output_tups

    # Pull all input tuples then sort
    def __pull_and_sort(self):
        tups = self.__pull_input_tups()
        while tups is not None:
            self.tups.extend(tups)
            tups = self.__pull_input_tups()
        self.tups = sorted(self.tups, key=self.comparator, reverse=not self.ASC)
        if self.propagate_prov:
            self.sorted_tups = [*self.tups]

    # Pull batch of input tuples
    def __pull_input_tups(self):
        tups = []
        finished = True
        for inp in self.inputs:
            next = inp.get_next()
            if next is not None:
                tups.extend(next)
                finished = False

        # Return None if done
        return None if finished else tups 

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        tuple_lineage = []
        if self.pull:
            for inp in self.inputs:
                tuple_lineage += inp.lineage(tuples)
        else:
            for output in self.outputs:
                tuple_lineage += output.lineage(tuples)
        
        return tuple_lineage

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        
        tuple_lineage = []
        if self.pull:
            for inp in self.inputs:
                tuple_lineage += inp.where(att_index, tuples)
        else:
            for output in self.outputs:
                tuple_lineage += output.where(att_index, tuples)
        
        return tuple_lineage

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        # Block until all tups are loaded and sorted
        if not self.loaded:
            if tuples is not None:
                self.tups.extend(tuples)
            else:
                self.loaded = True
                self.tups = sorted(self.tups, key=self.comparator, reverse=not self.ASC)
                if self.propagate_prov:
                    self.sorted_tups = [*self.tups]

        if self.loaded:
            # Output first batch_size items
            end_ind = min(self.batch_size, len(self.tups))
            output_tups = self.tups[:end_ind]
            # Remove output tups from list
            self.tups = self.tups[end_ind:]
            
            # Do this repeatedly for all batches (last apply call)
            while output_tups:
                for output in self.outputs:
                    output.apply(output_tups)

                # Grab next batch
                end_ind = min(self.batch_size, len(self.tups))
                output_tups = self.tups[:end_ind]
                self.tups = self.tups[end_ind:]

            for output in self.outputs:
                output.apply(None)

# Top-k operator
class TopK(Operator):
    """TopK operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        k (int): The maximum number of tuples to output.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes top-k operator
    def __init__(self,
                 inputs : List[Operator],
                 outputs : List[Operator],
                 k=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        super(TopK, self).__init__(name="TopK",
                                   track_prov=track_prov,
                                   propagate_prov=propagate_prov,
                                   pull=pull,
                                   partition_strategy=partition_strategy)
        # YOUR CODE HERE
        self.inputs = inputs
        self.outputs = outputs
        self.k = k
        self.i = 0
        self.batch_size = BATCH_SIZE

    # Returns the first k tuples in the input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        # Return None if done
        if self.i >= self.k: 
            return None

        tups = self.__pull_input_tups()

        # Return None if no more input
        if tups is None:
            return None

        # Output either batch_size or remaining tuples
        tups = tups[:min(len(tups), self.k - self.i)]
        self.i += len(tups) 
        return tups

    # Pull batch of input tuples
    def __pull_input_tups(self):
        tups = []
        finished = True
        for inp in self.inputs:
            next = inp.get_next()
            if next is not None:
                tups.extend(next)
                finished = False

        # Return None if done
        return None if finished else tups 

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        tuple_lineage = []
        if self.pull:
            for inp in self.inputs:
                tuple_lineage += inp.lineage(tuples)
        else:
            for output in self.outputs:
                tuple_lineage += output.lineage(tuples)
        
        return tuple_lineage

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        tuple_lineage = []
        if self.pull:
            for inp in self.inputs:
                tuple_lineage += inp.where(att_index, tuples)
        else:
            for output in self.outputs:
                tuple_lineage += output.where(att_index, tuples)
        
        return tuple_lineage

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        
        # None if done
        if self.i >= self.k or tuples is None:
            for output in self.outputs:
                output.apply(None)
            return
            
        # Output either batch_size or remaining tuples
        tuples = tuples[:min(len(tuples), self.k - self.i)]
        self.i += len(tuples) 
        for output in self.outputs:
            output.apply(tuples)

# Filter operator
class Select(Operator):
    """Select operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        predicate (function): The selection predicate.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes select operator
    def __init__(self,
                 inputs : List[Operator],
                 outputs : List[Operator],
                 predicate,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        super(Select, self).__init__(name="Select",
                                     track_prov=track_prov,
                                     propagate_prov=propagate_prov,
                                     pull=pull,
                                     partition_strategy=partition_strategy)
        # YOUR CODE HERE
        self.inputs = inputs
        self.outputs = outputs
        self.predicate = predicate
        self.batch_size = BATCH_SIZE
        self.join_output = None

    def set_join_output_side(self, is_right):
        self.join_output = is_right

    # Returns next batch of tuples that pass the filter (or None if done)
    def get_next(self):
        # YOUR CODE HERE

        tups = self.__pull_input_tups()
        # Return None if done
        if tups is None:
            return None

        selected_tups = self.__select(tups)
        
        return selected_tups

    # Pull batch of input tuples
    def __pull_input_tups(self):
        tups = []
        finished = True
        for inp in self.inputs:
            next = inp.get_next()
            if next:
                tups.extend(next)
                finished = False

        # Return None if done
        return None if finished else tups   

    # Selects input tuples using self.predicate
    def __select(self, tups):
        selected = []
        for tup in tups:
            # If tuple satisfies predicate, add to output
            if self.predicate(tup):
                selected.append(ATuple(tup.tuple, tup.metadata, self))

        return selected

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        
        tuple_lineage = []
        if self.pull:
            for inp in self.inputs:
                tuple_lineage += inp.lineage(tuples)
        else:
            for output in self.outputs:
                if self.join_output is None:
                    tuple_lineage += output.lineage(tuples)
                else:
                    tuple_lineage += output.lineage(tuples, self.join_output)
        
        return tuple_lineage

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)

        tuple_lineage = []
        if self.pull:
            for inp in self.inputs:
                tuple_lineage += inp.where(att_index, tuples)
        else:
            for output in self.outputs:
                if self.join_output is None:
                    tuple_lineage += output.where(att_index, tuples)
                else:
                    tuple_lineage += output.where(att_index, tuples, self.join_output)
        
        return tuple_lineage

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        
        # Return none if done
        tups = self.__select(tuples) if tuples is not None else None

        # Send to output ops
        for output in self.outputs:
            if self.join_output is None:
                output.apply(tups)
            # Join output
            else:
                output.apply(tups, self.join_output)

# Custom sink operator
class Sink(Operator):
    """Custom Sink operator. Stores output tuples and implements a similar pull-based wrapper.

    Attributes:
        start_scans (List): A list of handles to the instances of the scan 
        operators.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes sink operator
    def __init__(self,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        super(Sink, self).__init__(name="Sink",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov,
                                      pull=pull,
                                      partition_strategy=partition_strategy)
        # YOUR CODE HERE
        assert not pull
        self.started = False
        self.batches = []
        self.start_scans = []

    def set_start_scans(self, start_scans):
        self.start_scans = start_scans

    # Return next batch of tuples (or None if done)
    def get_next(self):
        # YOUR CODE HERE

        if not self.started:
            for op in self.start_scans:
                op.start()
            self.started = True

        if self.batches is None: 
            return None

        next_batch = self.batches[0]
        self.batches = self.batches[1:] if len(self.batches) > 1 else None
        return next_batch

    # Returns the lineage of the given tuples
    def lineage(self, tuples, is_start=False):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        
        if is_start:
            tuple_lineage = []
            for inp in self.start_scans:
                tuple_lineage += inp.lineage(tuples)
        else:
            tuple_lineage = tuples
        
        return tuple_lineage

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples, is_start=False):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)

        if is_start:
            tuple_lineage = []
            for inp in self.start_scans:
                tuple_lineage += inp.where(att_index, tuples)
        else:
            # Adjust tuples (tuple, att_index)
            tuple_lineage = []
            for tup in tuples:
                tuple_lineage.append(
                    ATuple((tup.tuple, att_index), 
                            tup.metadata, 
                            tup.operator)
                )
        
        return tuple_lineage

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        # Store all pushed tuples
        if tuples:
            tuples = [ATuple(tup.tuple, tup.metadata, self) for tup in tuples]
        self.batches.append(tuples)

# Using the root operation, pulls all input tuples and writes them to CSV
def output_to_csv(csv_fname, col_names, root_op):
    with open(csv_fname, mode='w') as file:
        writer = csv.writer(file, delimiter=',')
        writer.writerow(col_names)

        next = root_op.get_next()
        while next is not None:
            writer.writerows([x.tuple for x in next])
            next = root_op.get_next()

def output_to_pdf(fname, tuples):
    with open(fname, mode='w') as file:
        file.write(str(tuples))

# Using the root operation, pulls all input tuples
def process_query(root_op):
    tuples = []
    next = root_op.get_next()
    while next is not None:
        tuples += next
        next = root_op.get_next()

    logger.debug('Output Tuples:{}'.format(tuples))

    return tuples

def get_lineage(fname, root_op, pull, lineage):
    tuples = process_query(root_op)
    assert len(tuples) > lineage

    output = tuples[lineage].lineage(is_start=not pull)
    logger.debug('Lineage:{}'.format(output))
    output_to_pdf(fname, output)

def get_where(fname, root_op, pull, where_row, where_attr):
    tuples = process_query(root_op)
    assert len(tuples) > where_row
    
    output = tuples[where_row].where(where_attr, is_start=not pull)
    logger.debug('Where:{}'.format(output))
    output_to_pdf(fname, output)

def get_how(fname, root_op, how):
    tuples = process_query(root_op)
    assert len(tuples) > how

    output = tuples[how].how()
    logger.debug('How:{}'.format(output))
    output_to_pdf(fname, output)

def get_responsibility(fname, root_op, order, scans, responsibility):
    tuples = process_query(root_op)
    assert len(tuples) > responsibility

    output = tuples[responsibility].responsible_inputs(order, scans)
    logger.debug('Responsible Inputs:{}'.format(output))
    output_to_pdf(fname, output)


def process_query1(ff, mf, uid, mid, pull, 
                where_row=-1, 
                where_attr=-1):
    # TASK 1: Implement 'likeness' prediction query for User A and Movie M
    #
    # SELECT AVG(R.Rating)
    # FROM Friends as F, Ratings as R
    # WHERE F.UID2 = R.UID
    #       AND F.UID1 = 'A'
    #       AND R.MID = 'M'
    tp = where_row > -1

    # YOUR CODE HERE
    if pull:
        sc1 = Scan(ff, None, track_prov=tp)
        sc2 = Scan(mf, None, track_prov=tp)

        se1 = Select([sc1], None, lambda x: x.tuple[0] == uid, track_prov=tp)
        se2 = Select([sc2], None, lambda x: x.tuple[1] == mid, track_prov=tp)

        join = Join([se1], [se2], None, 1, 0, track_prov=tp)
        avg = GroupBy([join], None, 2, 3, lambda x: sum(x) / len(x), track_prov=tp)
        root_op = Project([avg], None, [1], pull=pull, track_prov=tp)

    else:
        sink = Sink(pull=pull, track_prov=tp)
        project = Project(None, [sink], [1], pull=pull, track_prov=tp)
        avg = GroupBy(None, [project], 2, 3, lambda x: sum(x) / len(x), pull=pull, track_prov=tp) 
        join = Join(None, None, [avg], 1, 0, pull=pull, track_prov=tp)

        se1 = Select(None, [join], lambda x: x.tuple[0] == uid, pull=pull, track_prov=tp)
        se1.set_join_output_side(is_right=False)
        se2 = Select(None, [join], lambda x: x.tuple[1] == mid, pull=pull, track_prov=tp)
        se2.set_join_output_side(is_right=True)

        sc1 = Scan(ff, [se1], pull=pull, track_prov=tp)
        sc2 = Scan(mf, [se2], pull=pull, track_prov=tp)

        sink.set_start_scans([sc1, sc2])
        root_op = sink

    if where_row == -1:
        output_to_csv(args.output, ["#", "likeness"], root_op)
    else:
        get_where(args.output, root_op, pull, where_row, where_attr)

def process_query2(ff, mf, uid, mid, pull,
                lineage=-1, how=-1, 
                responsibility=-1):
    # TASK 2: Implement recommendation query for User A
    #
    # SELECT R.MID
    # FROM ( SELECT R.MID, AVG(R.Rating) as score
    #        FROM Friends as F, Ratings as R
    #        WHERE F.UID2 = R.UID
    #              AND F.UID1 = 'A'
    #        GROUP BY R.MID
    #        ORDER BY score DESC
    #        LIMIT 1 )

    tp = lineage > -1
    pp = how > -1 or responsibility > -1

    # YOUR CODE HERE
    if pull:
        sc1 = Scan(ff, None, table_name='Friends', track_prov=tp, propagate_prov=pp)
        sc2 = Scan(mf, None, table_name='Ratings', track_prov=tp, propagate_prov=pp)

        se1 = Select([sc1], None, lambda x: x.tuple[0] == uid, track_prov=tp, propagate_prov=pp)

        join = Join([se1], [sc2], None, 1, 0, track_prov=tp, propagate_prov=pp)
        avg = GroupBy([join], None, 2, 3, lambda x: sum(x) / len(x), agg_fun_name='AVG', track_prov=tp, propagate_prov=pp)

        order = OrderBy([avg], None, lambda x: x.tuple[1], ASC=False, track_prov=tp, propagate_prov=pp)
        limit = TopK([order], None, 1, track_prov=tp, propagate_prov=pp)

        root_op = Project([limit], None, [0], 
                        track_prov=tp, propagate_prov=pp)

    else:
        sink = Sink(pull=pull, track_prov=tp, propagate_prov=pp)

        project = Project(None, [sink], [0], pull=pull, track_prov=tp, propagate_prov=pp)
        limit = TopK(None, [project], 1, pull=pull, track_prov=tp, propagate_prov=pp)

        order = OrderBy(None, [limit], lambda x: x.tuple[1], ASC=False, 
                        pull=pull, track_prov=tp, propagate_prov=pp)

        avg = GroupBy(None, [order], 2, 3, lambda x: sum(x) / len(x), 
                        agg_fun_name='AVG',
                        pull=pull, track_prov=tp, propagate_prov=pp)
        join = Join(None, None, [avg], 1, 0, pull=pull, track_prov=tp, propagate_prov=pp)

        se1 = Select(None, [join], lambda x: x.tuple[0] == uid, 
                        pull=pull, track_prov=tp, propagate_prov=pp)
        se1.set_join_output_side(is_right=False)

        sc1 = Scan(ff, [se1], table_name='Friends', pull=pull, track_prov=tp, propagate_prov=pp)
        sc2 = Scan(mf, [join], table_name='Ratings', pull=pull, track_prov=tp, propagate_prov=pp)
        sc2.set_join_output_side(is_right=True)

        sink.set_start_scans([sc1, sc2])
        root_op = sink

    # Typical output
    if lineage == -1 and how == -1 and responsibility == -1:
        output_to_csv(args.output, ["#", "MID"], sink)
    elif lineage > -1:
        get_lineage(args.output, root_op, pull=pull, lineage=lineage)
    elif how > -1:
        get_how(args.output, root_op, how=how)
    elif responsibility > -1:
        get_responsibility(args.output, root_op, order, [sc1, sc2], responsibility=responsibility)

    # get_where(root_op, pull=False)

    # get_where(project, pull=True)

def process_query3(ff, mf, uid, mid, pull):
    # TASK 3: Implement explanation query for User A and Movie M
    #
    # SELECT HIST(R.Rating) as explanation
    # FROM Friends as F, Ratings as R
    # WHERE F.UID2 = R.UID
    #       AND F.UID1 = 'A'
    #       AND R.MID = 'M'

    # YOUR CODE HERE
    if pull:
        sc1 = Scan(ff, None)
        sc2 = Scan(mf, None)

        se1 = Select([sc1], None, lambda x: x.tuple[0] == uid)
        se2 = Select([sc2], None, lambda x: x.tuple[1] == mid)

        join = Join([se1], [se2], None, 1, 0)
        hist = Histogram([join], None, 3)

        output_to_csv(args.output, ["#", "explanation"], hist)

    else:
        
        sink = Sink(pull=pull)
        hist = Histogram(None, [sink], 3, pull=pull)
        join = Join(None, None, [hist], 1, 0, pull=pull)

        se1 = Select(None, [join], lambda x: x.tuple[0] == uid, pull=pull)
        se1.set_join_output_side(is_right=False)
        se2 = Select(None, [join], lambda x: x.tuple[1] == mid, pull=pull)
        se2.set_join_output_side(is_right=True)

        sc1 = Scan(ff, [se1], pull=pull)
        sc2 = Scan(mf, [se2], pull=pull)

        sink.set_start_scans([sc1, sc2])
        output_to_csv(args.output, ["#", "explanation"], sink)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Assignement 1&2.')
    parser.add_argument('--query', type=int, help='Which query to run [1 / 2 / 3].')
    parser.add_argument('--ff', type=str, default='../data/friends-test.txt', help='Friends file path.')
    parser.add_argument('--mf', type=str, default='../data/movie_ratings-test.txt', help='Movie file path.')
    parser.add_argument('--uid', type=int, default=1190, help='uid.')
    parser.add_argument('--mid', type=int, default=0, help='mid.')
    parser.add_argument('--pull', type=int, default=1, help='0/1 - 0 Push, 1 Pull')
    parser.add_argument('--output', type=str, default='../data/output.txt', help='File path.')

    parser.add_argument('--lineage', type=int, default=-1, help='The index of the tuple in the result to find the lineage')
    parser.add_argument('--where-row', type=int, default=-1, help='The index of the tuple in the result to find the where provenance')
    parser.add_argument('--where-attribute', type=int, default=-1, help='The index of the attribute to find the where provenance')
    parser.add_argument('--how', type=int, default=-1, help='The index of the tuple in the result to find the how provenance')
    parser.add_argument('--responsibility', type=int, default=-1, help='The index of the tuple in the result to find the responsibility')

    args = parser.parse_args()

    logger.info("Assignment #1")

    if args.query == 1:
        process_query1(args.ff, args.mf, args.uid, args.mid, args.pull==1,
                    where_row=args.where_row, where_attr=args.where_attribute)
    elif args.query == 2:
        process_query2(args.ff, args.mf, args.uid, args.mid, args.pull==1,
                    lineage=args.lineage, how=args.how, responsibility=args.responsibility)
    elif args.query == 3:
        process_query3(args.ff, args.mf, args.uid, args.mid, args.pull==1)
    else:
        logger.error("Only queries 1/2/3 implemented")

    logger.info("Assignment #2")

    # TODO:
        # Input/output
        # Comments
        # Query 1
        # Tests

    # TASK 1: Implement lineage query for movie recommendation

    # YOUR CODE HERE


    # TASK 2: Implement where-provenance query for 'likeness' prediction

    # YOUR CODE HERE


    # TASK 3: Implement how-provenance query for movie recommendation

    # YOUR CODE HERE


    # TASK 4: Retrieve most responsible tuples for movie recommendation

    # YOUR CODE HERE
