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
    def lineage(self) -> List[ATuple]:
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the Where-provenance of the attribute at index 'att_index' of self
    def where(self, att_index) -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Returns the How-provenance of self
    def how(self) -> string:
        # YOUR CODE HERE (ONLY FOR TASK 3 IN ASSIGNMENT 2)
        pass

    # Returns the input tuples with responsibility \rho >= 0.5 (if any)
    def responsible_inputs(self) -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 4 IN ASSIGNMENT 2)
        pass

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
        return [ATuple(i[:-1], None, self) for i in list(df_batch.itertuples(index=False, name=None))]

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

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
        # Remove join attribute from right tuple
        if is_right:
            right_data = [probe_tup.tuple[i] for i in range(len(probe_tup.tuple)) if i != self.right_join_attribute]
            joined_data = tuple([x for x in hashed_tup.tuple] + right_data)
        else:
            right_data = [hashed_tup.tuple[i] for i in range(len(hashed_tup.tuple)) if i != self.right_join_attribute]
            joined_data = tuple([x for x in probe_tup.tuple] + right_data)

        return ATuple(joined_data, probe_tup.metadata, self)

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

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
            tups[i] = projected
        
    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

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
        self.groups = dict() # Stores {group: {sum: x, n: x}}
        self.grouped = False
        
        self.batch_size = BATCH_SIZE

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

            if key in self.groups:
                self.groups[key]['sum'] += data[self.value]
                self.groups[key]['n'] += 1
            else:
                self.groups[key] = {'sum': data[self.value], 
                                    'n': 1}

    # Create our new output tuples from group stats
    def __create_tuples_from_stats(self):
        # No groups left
        if len(self.groups) == 0:
            return None
        
        tups, groups_to_remove = [], []
        for i, (group, stat) in enumerate(self.groups.items()):
            tups.append(ATuple((group, self.agg_fun(stat),), 
                                    None, self))

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
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

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
            else:
                self.buckets[key] = 1

    # Create our new output tuples from group stats
    def __create_tuples_from_stats(self):
        # No buckets left
        if self.buckets is None:
            return None

        tups = [ATuple((self.buckets,), None, self)]
        self.buckets = None
        
        # TODO: Left in because I'm not sure what the output should be
        # tups, buckets_to_remove = [], []
        # for i, (bucket, stat) in enumerate(self.buckets.items()):
        #     tups.append(ATuple(('{}:{}'.format(bucket, stat),), None, self))
        #     buckets_to_remove.append(bucket)
        #     if i == self.batch_size - 1: break
        # # Remove buckets from dict
        # for i in range(len(buckets_to_remove)):
        #     del self.buckets[buckets_to_remove[i]]
            
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
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        # Block until all tups are loaded and sorted
        if not self.loaded:
            if tuples is not None:
                self.tups.extend(tuples)
            else:
                self.loaded = True
                self.tups = sorted(self.tups, key=self.comparator, reverse=not self.ASC)

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
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

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
            # TODO: Recreate ATuple here with new operator
            # If tuple satisfies predicate, add to output
            if self.predicate(tup):
                selected.append(tup)

        return selected

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

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        # Store all pushed tuples
        self.batches.append(tuples)

# Using the root operation, pulls all input tuples and writes them to CSV
def output_to_csv(csv_fname, col_names, root_op):
    with open(csv_fname, mode='w') as file:
        writer = csv.writer(file, delimiter=' ')
        writer.writerow(col_names)

        next = root_op.get_next()
        while next is not None:
            writer.writerows([x.tuple for x in next])
            next = root_op.get_next()


def process_query1(ff, mf, uid, mid, pull):
    # TASK 1: Implement 'likeness' prediction query for User A and Movie M
    #
    # SELECT AVG(R.Rating)
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
        avg = GroupBy([join], None, 2, 3, lambda x: x['sum'] / x['n'])
        project = Project([avg], None, [1], pull=pull)

        output_to_csv(args.output, ["#", "likeness"], project)

    else:
        sink = Sink(pull=pull)
        project = Project(None, [sink], [1], pull=pull)
        avg = GroupBy(None, [project], 2, 3, lambda x: x['sum'] / x['n'], pull=pull) 
        join = Join(None, None, [avg], 1, 0, pull=pull)

        se1 = Select(None, [join], lambda x: x.tuple[0] == uid, pull=pull)
        se1.set_join_output_side(is_right=False)
        se2 = Select(None, [join], lambda x: x.tuple[1] == mid, pull=pull)
        se2.set_join_output_side(is_right=True)

        sc1 = Scan(ff, [se1], pull=pull)
        sc2 = Scan(mf, [se2], pull=pull)

        sink.set_start_scans([sc1, sc2])
        output_to_csv(args.output, ["#", "likeness"], sink)

def process_query2(ff, mf, uid, mid, pull):
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

    # YOUR CODE HERE
    if pull:
        sc1 = Scan(ff, None)
        sc2 = Scan(mf, None)

        se1 = Select([sc1], None, lambda x: x.tuple[0] == uid)

        join = Join([se1], [sc2], None, 1, 0)
        avg = GroupBy([join], None, 2, 3, lambda x: x['sum'] / x['n'])

        order = OrderBy([avg], None, lambda x: x.tuple[1], ASC=False)
        limit = TopK([order], None, 1)

        project = Project([limit], None, [0])

        output_to_csv(args.output, ["#", "MID"], project)

    else:
        sink = Sink(pull=pull)

        project = Project(None, [sink], [0], pull=pull)
        limit = TopK(None, [project], 1, pull=pull)

        order = OrderBy(None, [limit], lambda x: x.tuple[1], ASC=False, pull=pull)

        avg = GroupBy(None, [order], 2, 3, lambda x: x['sum'] / x['n'], pull=pull)
        join = Join(None, None, [avg], 1, 0, pull=pull)

        se1 = Select(None, [join], lambda x: x.tuple[0] == uid, pull=pull)
        se1.set_join_output_side(is_right=False)

        sc1 = Scan(ff, [se1], pull=pull)
        sc2 = Scan(mf, [join], pull=pull)
        sc2.set_join_output_side(is_right=True)

        sink.set_start_scans([sc1, sc2])

        output_to_csv(args.output, ["#", "MID"], sink)

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
    args = parser.parse_args()

    logger.info("Assignment #1")

    if args.query == 1:
        process_query1(args.ff, args.mf, args.uid, args.mid, args.pull==1)
    elif args.query == 2:
        process_query2(args.ff, args.mf, args.uid, args.mid, args.pull==1)
    elif args.query == 3:
        process_query3(args.ff, args.mf, args.uid, args.mid, args.pull==1)
    else:
        logger.error("Only queries 1/2/3 implemented")

    # TODO:
    # TASK 4: Turn your data operators into Ray actors
    #
    # NOTE (john): Add your changes for Task 4 to a new git branch 'ray'
    # Select/Join/Group By


    logger.info("Assignment #2")

    # TASK 1: Implement lineage query for movie recommendation

    # YOUR CODE HERE


    # TASK 2: Implement where-provenance query for 'likeness' prediction

    # YOUR CODE HERE


    # TASK 3: Implement how-provenance query for movie recommendation

    # YOUR CODE HERE


    # TASK 4: Retrieve most responsible tuples for movie recommendation

    # YOUR CODE HERE
