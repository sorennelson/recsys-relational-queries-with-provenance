
import pytest

from assignment_12 import ATuple, Scan, Join, Project, GroupBy, Histogram, OrderBy, TopK, Select, Sink

left = [ATuple((1190, 15)), ATuple((1020, 1095)), ATuple((1190, 1095)), ATuple((1190, 6))]
right = [ATuple((5, 1, 2)), ATuple((6, 0, 4)), ATuple((6, 1, 0)),
            ATuple((1095, 0, 5)), ATuple((1095, 2, 5))]

class TestOperators:

    def __pull(self, op):
        x = []
        next = op.get_next()
        while next is not None:
            x.extend(next)
            next = op.get_next()

        return x

    # Scan
    def test_scan(self):
        op = Scan('../data/friends.txt', None)
        x = self.__pull(op)
        assert len(x) == 78991
        
    # Join
    def test_join(self):
        # Join tuples to find friend's ratings
        ans = [(1190, 6, 0, 4), (1190, 6, 1, 0), (1020, 1095, 0, 5), 
                (1190, 1095, 0, 5), (1020, 1095, 2, 5), (1190, 1095, 2, 5)]

        sink = Sink(pull=False)
        op = Join(None, None, [sink], 1, 0, pull=False)
        
        op.apply(left, is_right=False)
        op.apply(right, is_right=True)
        # NOTE (soren): Not needed but for consistency
        op.apply(None, is_right=False)
        op.apply(None, is_right=True)
        
        output = self.__pull(sink)
        assert all([x.tuple in ans for x in output]) and len(ans) == len(output)

    # Project
    def test_project(self):
        # All Friends
        ans = [(15,), (1095,), (1095,), (6,)]

        sink = Sink(pull=False)
        op = Project(None, [sink], [1], pull=False)
        op.apply(left)
        # NOTE (soren): Not needed but for consistency
        op.apply(None)

        output = self.__pull(sink)
        assert all([x.tuple in ans for x in output]) and len(ans) == len(output)
        
    # GroupBy
    def test_groupby(self):
        # Average rating per movie
        ans = [(0, 4.5), (1, 1), (2, 5)]

        sink = Sink(pull=False)
        op = GroupBy(None, [sink], 1, 2, lambda x: sum(x) / len(x), pull=False)
        op.apply(right)
        # Have to finish aggregation
        op.apply(None)

        output = self.__pull(sink)
        assert all([x.tuple in ans for x in output]) and len(ans) == len(output)

    # Histogram
    def test_histogram(self):
        # Histogram of ratings per movie
        ans = [(0,2,), (1,2,), (2,1,)]

        sink = Sink(pull=False)
        op = Histogram(None, [sink], 1, lambda x: x['sum'] / x['n'], pull=False)
        op.apply(right)
        # Have to finish counting
        op.apply(None)

        output = self.__pull(sink)
        assert all([x.tuple in ans for x in output]) and len(ans) == len(output)

    # OrderBy
    def test_orderby(self):
        # Sort movies by movie-id
        ans = [ATuple((6, 0, 4)), ATuple((1095, 0, 5)), ATuple((5, 1, 2)), 
                ATuple((6, 1, 0)), ATuple((1095, 2, 5))]

        sink = Sink(pull=False)
        op = OrderBy(None, [sink], lambda x: x.tuple[1], pull=False)
        op.apply(right)
        # Have to finish so we can sort
        op.apply(None)

        output = self.__pull(sink)
        assert all([output[i].tuple == ans[i].tuple for i in range(len(output))]) and len(ans) == len(output)

    # TopK
    def test_topk(self):
        # Top-k sorted movies by movie-id
        ans = [ATuple((6, 0, 4)), ATuple((1095, 0, 5)), ATuple((5, 1, 2))]

        sink = Sink(pull=False)
        op = TopK(None, [sink], 3, pull=False)
        op2 = OrderBy(None, [op], lambda x: x.tuple[1], pull=False)
        op2.apply(right)
        # Have to finish so we can sort
        op2.apply(None)

        output = self.__pull(sink)
        assert all([output[i].tuple == ans[i].tuple for i in range(len(output))]) and len(ans) == len(output)

    # Select
    def test_select(self):
        # Select all ratings of movies 0
        ans = [(6, 0, 4), (1095, 0, 5)]

        sink = Sink(pull=False)
        op = Select(None, [sink], lambda x: x.tuple[1] == 0, pull=False)
        op.apply(right)
        # NOTE (soren): Not needed but for consistency
        op.apply(None)

        output = self.__pull(sink)
        assert all([x.tuple in ans for x in output]) and len(ans) == len(output)

    def __get_query_1(self, ff, mf, uid, mid, tp):
        sc1 = Scan(ff, None, track_prov=tp)
        sc2 = Scan(mf, None, track_prov=tp)

        se1 = Select([sc1], None, lambda x: x.tuple[0] == uid, track_prov=tp)
        se2 = Select([sc2], None, lambda x: x.tuple[1] == mid, track_prov=tp)

        join = Join([se1], [se2], None, 1, 0, track_prov=tp)
        avg = GroupBy([join], None, 2, 3, lambda x: sum(x) / len(x), track_prov=tp)
        root_op = Project([avg], None, [1], pull=True, track_prov=tp)

        return root_op

    def __get_query_2(self, ff, mf, uid, tp, pp):
        sc1 = Scan(ff, None, table_name='Friends', track_prov=tp, propagate_prov=pp)
        sc2 = Scan(mf, None, table_name='Ratings', track_prov=tp, propagate_prov=pp)

        se1 = Select([sc1], None, lambda x: x.tuple[0] == uid, track_prov=tp, propagate_prov=pp)

        join = Join([se1], [sc2], None, 1, 0, track_prov=tp, propagate_prov=pp)
        avg = GroupBy([join], None, 2, 3, lambda x: sum(x) / len(x), agg_fun_name='AVG', track_prov=tp, propagate_prov=pp)

        order = OrderBy([avg], None, lambda x: x.tuple[1], ASC=False, track_prov=tp, propagate_prov=pp)
        limit = TopK([order], None, 1, track_prov=tp, propagate_prov=pp)

        root_op = Project([limit], None, [0], 
                        track_prov=tp, propagate_prov=pp)

        return root_op, order, [sc1, sc2]
                    
    def test_lineage(self):
        tp = True
        pp = False
        ff = '../data/friends-test.txt'
        mf = '../data/movie_ratings-test.txt'
        lineage = 0
        uid = 1190

        root_op, _, _ = self.__get_query_2(ff, mf, uid, tp, pp)
        tuples = self.__pull(root_op)
        assert len(tuples) > lineage

        output = tuples[lineage].lineage(is_start=False)
        assert [t.tuple for t in output] == [(1190, 6), (1190, 1095), (6, 2, 5), (1095, 2, 5)]

    def test_where(self):
        tp = True
        ff = '../data/friends-test.txt'
        mf = '../data/movie_ratings-test.txt'
        where_row = 0
        where_attr = 0
        uid = 1190
        mid = 0

        root_op = self.__get_query_1(ff, mf, uid, mid, tp)
        tuples = self.__pull(root_op)
        assert len(tuples) > where_row

        output = tuples[where_row].where(where_attr, is_start=False)
        assert str(output) == str([('movie_ratings-test.txt', 7, (6, 0, 4), 4), ('movie_ratings-test.txt', 10, (1095, 0, 5), 5)])

    def test_how(self):
        tp = False
        pp = True
        ff = '../data/friends-test.txt'
        mf = '../data/movie_ratings-test.txt'
        how = 0
        uid = 1190

        root_op, _, _ = self.__get_query_2(ff, mf, uid, tp, pp)
        tuples = self.__pull(root_op)
        assert len(tuples) > how

        output = tuples[how].how()
        assert output == "AVG( (f4*r9@5), (f3*r12@5) )"

    def test_responsibility(self):
        tp = False
        pp = True
        ff = '../data/friends-test.txt'
        mf = '../data/movie_ratings-test.txt'
        responsibility = 0
        uid = 1190

        root_op, order, scans = self.__get_query_2(ff, mf, uid, tp, pp)
        tuples = self.__pull(root_op)
        assert len(tuples) > responsibility

        output = tuples[responsibility].responsible_inputs(order, scans)
        assert str(output) == str([((1190, 6), 0.5), ((6, 2, 5), 0.5), ((1190, 1095), 0.5), ((1095, 2, 5), 0.5)])