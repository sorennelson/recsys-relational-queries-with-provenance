
import pytest
import ray

from assignment_12 import ATuple, Scan, Join, Project, GroupBy, Histogram, OrderBy, TopK, Select, Sink

left = [ATuple((1190, 15)), ATuple((1020, 1095)), ATuple((1190, 1095)), ATuple((1190, 6))]
right = [ATuple((5, 1, 2)), ATuple((6, 0, 4)), ATuple((6, 1, 0)),
            ATuple((1095, 0, 5)), ATuple((1095, 2, 5))]

class TestOperators:

    def __pull(self, op):
        x = []
        next = ray.get(op.get_next.remote())
        while next is not None:
            x.extend(next)
            next = ray.get(op.get_next.remote())

        return x
        
    # Join
    def test_join(self):
        # Join tuples to find friend's ratings
        ans = [(1190, 6, 0, 4), (1190, 6, 1, 0), (1020, 1095, 0, 5), 
                (1190, 1095, 0, 5), (1020, 1095, 2, 5), (1190, 1095, 2, 5)]

        sink = Sink.remote(num_input=1, pull=False)
        op = Join.remote(None, None, [sink], 1, 0, pull=False)
        
        op.apply.remote(left, is_right=False)
        op.apply.remote(right, is_right=True)
        # NOTE (soren): Not needed but for consistency
        op.apply.remote(None, is_right=False)
        ray.get(op.apply.remote(None, is_right=True))
        
        output = self.__pull(sink)
        assert all([x.tuple in ans for x in output]) and len(ans) == len(output)

    # Project
    def test_project(self):
        # All Friends
        ans = [(15,), (1095,), (1095,), (6,)]

        sink = Sink.remote(num_input=1, pull=False)
        op = Project.remote(None, [sink], [1], pull=False)

        op.apply.remote(left)
        # NOTE (soren): Not needed but for consistency
        ray.get(op.apply.remote(None))

        output = self.__pull(sink)
        assert all([x.tuple in ans for x in output]) and len(ans) == len(output)
        
    # GroupBy
    def test_groupby(self):
        # Average rating per movie
        ans = [(0, 4.5), (1, 1), (2, 5)]

        sink = Sink.remote(num_input=1, pull=False)
        op = GroupBy.remote(None, [sink], 1, 2, lambda x: sum(x) / len(x), pull=False)

        op.apply.remote(right)
        # Have to finish aggregation
        ray.get(op.apply.remote(None))

        output = self.__pull(sink)
        assert all([x.tuple in ans for x in output]) and len(ans) == len(output)

    # Histogram
    def test_histogram(self):
        # Histogram of ratings per movie
        ans = [(0,2,), (1,2,), (2,1,)]

        sink = Sink.remote(num_input=1, pull=False)
        op = Histogram.remote(None, [sink], 1, 'AVG', pull=False)
        op.apply.remote(right)
        # Have to finish counting
        ray.get(op.apply.remote(None))

        output = self.__pull(sink)
        assert all([x.tuple in ans for x in output]) and len(ans) == len(output)

    # OrderBy
    def test_orderby(self):
        # Sort movies by movie-id
        ans = [ATuple((6, 0, 4)), ATuple((1095, 0, 5)), ATuple((5, 1, 2)), 
                ATuple((6, 1, 0)), ATuple((1095, 2, 5))]

        sink = Sink.remote(num_input=1, pull=False)
        op = OrderBy.remote(None, [sink], lambda x: x.tuple[1], pull=False)
        op.apply.remote(right)
        # Have to finish so we can sort
        ray.get(op.apply.remote(None))

        output = self.__pull(sink)
        assert all([output[i].tuple == ans[i].tuple for i in range(len(output))]) and len(ans) == len(output)

    # TopK
    def test_topk(self):
        # Top-k sorted movies by movie-id
        ans = [ATuple((6, 0, 4)), ATuple((1095, 0, 5)), ATuple((5, 1, 2))]

        sink = Sink.remote(num_input=1, pull=False)
        op = TopK.remote(None, [sink], 3, pull=False)
        op2 = OrderBy.remote(None, [op], lambda x: x.tuple[1], pull=False)
        op2.apply.remote(right)
        # Have to finish so we can sort
        ray.get(op2.apply.remote(None))

        output = self.__pull(sink)
        assert all([output[i].tuple == ans[i].tuple for i in range(len(output))]) and len(ans) == len(output)

    # Select
    def test_select(self):
        # Select all ratings of movies 0
        ans = [(6, 0, 4), (1095, 0, 5)]

        sink = Sink.remote(num_input=1, pull=False)
        op = Select.remote(None, [sink], lambda x: x.tuple[1] == 0, pull=False)
        op.apply.remote(right)
        # NOTE (soren): Not needed but for consistency
        ray.get(op.apply.remote(None))

        output = self.__pull(sink)
        assert all([x.tuple in ans for x in output]) and len(ans) == len(output)