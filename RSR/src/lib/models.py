import numpy as np
from datetime import datetime, date
from scipy.stats import nct
from scipy.linalg import solve_triangular
import time as clock
import random
from opentelemetry import trace

class Model:
    """
    Interface for ML model
    """
    def apply_model(self, data, times, top_left) -> bool:
        pass


####################################################################################
####################################################################################
# Functions for phenology regression
####################################################################################
####################################################################################

def lm(X, Xt, y, weights, n):
    W = weights * np.eye(n)
    Sinv = Xt @ W @ X
    L = np.linalg.cholesky(Sinv)
    XtWy = Xt @ W @ y
    temp = np.linalg.solve(L, XtWy)
    B = np.linalg.solve(np.transpose(L), temp)
    y_XB = y - X @ B
    sq_res = (y_XB) ** 2.
    sigma = np.sqrt(np.transpose(y_XB) @ W @ y_XB / n)
    return ([sq_res, sigma, B])


def design_matrix(time, harmonics, period, n):
    ones = np.ones(n)
    X = np.column_stack((ones, time))
    for i in range(harmonics):
        w = (2. * np.pi * time * (i + 1) / period).reshape(n, 1)
        X = np.concatenate((X, np.cos(w), np.sin(w)), axis=1)
    return (X)


def phenology_regression(X, y, nu, harmonics, period, maxit=15, epsilon=1, trace=True):
    n = len(y)
    # X   = design_matrix(time, harmonics, period,n) # initialize data
    Xt = np.transpose(X)
    Q_t = 0  # complete data likelihood at iteration t
    Eq = np.ones(n)  # initialize latent variables
    #
    for it in range(maxit):
        # [ M-step]
        sq_res, sigma, B = lm(X, Xt, y, Eq, n)
        Q_tplus1 = np.sum(np.log(sigma) + (Eq * sq_res / (2 * sigma ** 2)))  # Compute the complete data log likelihood
        # [ E-step ]
        Eq = (nu + 1) / (nu + ((sq_res) / (sigma ** 2)))
        if trace: print("[", it, "] Q_t = ", Q_tplus1)
        if (it > 3) & ((Q_t - Q_tplus1) / abs(Q_t + .01) < epsilon):  # Check convergence
            Q_t = Q_tplus1
            break
        Q_t = Q_tplus1
    return (Q_t)


####################################################################################
####################################################################################
# Functions for change point detection
####################################################################################
####################################################################################

####################################################################################
# Functions for change point detection
####################################################################################
####################################################################################

def frobenius_norm(A):
    return (np.sqrt(np.sum(A ** 2)))


def grad_l(p, B, Xt, c, L, Z):
    return (Xt @ (Z - p) - L @ (B - c))


def glm(n, Z, X, Xt, XXtX_inv, L, family='quasibinomial'):
    c = np.array([-2.6, 5.2 / n])  # /n]) #np.array([5.6,105.2/n])
    B = np.array([0., 0.])
    p = plogis(X @ B)
    W = p * np.eye(n)
    W_inv = (1 / p) * np.eye(n)
    #
    for k in range(200):
        Low = np.linalg.cholesky(Xt @ W @ X + L)
        temp = solve_triangular(Low, Xt @ W @ (X @ B + W_inv @ (Z - p) + W_inv @ XXtX_inv @ L @ c), lower=True)
        B = solve_triangular(np.transpose(Low), temp, lower=False)
        p = plogis(X @ B)
        W = p * np.eye(n)
        W_inv = (1 / p) * np.eye(n)
        if frobenius_norm(grad_l(p, B, Xt, c, L, Z)) < (n * 5):
            break
    #
    return ([p, B])


def logit(p):
    return (np.log(p / (1 - p)))


def plogis(x):
    return (1. / (1. + np.exp(-x)))


def bayes_lm(X, Xt, y, weights, denom_w, n):
    lam = 1
    W = weights * np.eye(n)
    Sinv = Xt @ W @ X + lam * np.eye(X.shape[1])
    L = np.linalg.cholesky(Sinv)
    XtWy = Xt @ W @ y
    temp = np.linalg.solve(L, XtWy)
    B = np.linalg.solve(np.transpose(L), temp)
    y_XB = y - X @ B
    sq_res = (y_XB) ** 2.
    sigma = np.sqrt(np.transpose(y_XB) @ W @ y_XB / np.sum(denom_w))
    return ([sq_res, sigma, B])

def phenology_smooch(time, X, y, nu, harmonics, period, maxit=5, epsilon=1, trace=False):
    # Set up frames and constants:
    n = len(y)
    # X     = design_matrix(time, harmonics, period,n) # initialize data
    Xt = np.transpose(X)
    # Quasibinomial frames and constants:
    T = np.column_stack((np.ones(n), time))
    Tt = np.transpose(T)
    L = np.array([[1e2, 0.], [0., 1e-10]])  # prior to make sure change falls within time
    #
    TTtT_inv = T @ np.linalg.pinv(Tt @ T, hermitian=True)
    #
    # EM initializations:
    Q_t = 0
    delta = (max(time) - min(time)) / 5.
    tau = max(time) / 2.
    Ez = plogis((time - delta) / tau)  # 0.5*np.ones(n)
    Eqz = np.random.gamma(nu / 2., 1 / 2., n) * Ez  # rgamma(n,nu/2,1/2)*Ez
    Eq1mz = np.random.gamma(nu / 2., 1 / 2., n) * Ez
    #
    for it in range(maxit):
        # [ M-step]
        sq_res0, sigma0, B0 = bayes_lm(X, Xt, y, Eq1mz, (1 - Ez), n)
        sq_res1, sigma1, B1 = bayes_lm(X, Xt, y, Eqz, Ez, n)
        p, B_quas = glm(n, Ez, T, Tt, TTtT_inv, L, family='quasibinomial')
        p = p / (1 + 1e-15)  # temp solution against log(0)
        #
        Q_tplus1 = -np.sum(
            (1 - Ez) * (np.log(1 - p) - np.log(sigma0)) - (Eq1mz * sq_res0 / (2 * sigma0 ** 2.))
            + Ez * (np.log(p) - np.log(sigma1)) - (Eqz * sq_res1 / (2 * sigma1 ** 2.)))
        # [ E-step ]
        Ez = plogis(logit(p)
                    + nct.logpdf(x=y, df=nu, loc=X @ B1, scale=sigma1, nc=0)
                    - nct.logpdf(x=y, df=nu, loc=X @ B0, scale=sigma0, nc=0)
                    )
        Eqz = Ez * (nu + 1) / (nu + (sq_res1 / (sigma1 ** 2)))  # Eq = (nu+1)/(nu+((sq_res)/(sigma**2)))
        Eq1mz = (1 - Ez) * (nu + 1) / (nu + (sq_res0 / (sigma0 ** 2)))
        Eq = Eqz + Eq1mz
        #
        #
        if trace: print("[", it, "] Q_t = ", Q_tplus1)
        if (it > 5) & ((Q_t - Q_tplus1) / (Q_t + .01) < epsilon):  # Check convergence
            Q_t = Q_tplus1
            break
        Q_t = Q_tplus1
    ch_id = -B_quas[0] / B_quas[1]
    return ([Q_t, ch_id])


class RegressionModel(Model):
    """
    Model created by Dan
    """
    # Applies Dan's model to the time series partition
    # NOTE (john): This method is called after calling 'apply_model()' on the
    def apply_model(self, data, times, top_left) -> bool:
        grid_ts = data
        times = times
        top_left = top_left
        # Convert time to days since 01/01/1970
        start = date(1970, 1, 1)

        def str_to_date(a):
            return (datetime.strptime(a, '%Y%m%d').date())

        def date_to_days(a, start):
            return (a - start).days

        vstr_to_date = np.vectorize(str_to_date)
        vdate_to_days = np.vectorize(date_to_days)
        times = vstr_to_date(times)
        times = vdate_to_days(times, start)
        # Create template design matrix and transpose here, then slice out nans based on y
        n = len(times)
        logn = np.log(n)
        X_template = design_matrix(times, 3, 365, n)  # initialize data
        # Construct array for results storage.
        # We need a way to map results back to their coordinates
        results = np.empty([2, grid_ts.shape[1], grid_ts.shape[2]]).astype('uint16')
        print("Applying regression model to {} rows and {} columns".format(grid_ts.shape[1],grid_ts.shape[2]))
        start_time = clock.time()
        for i in range(grid_ts.shape[1]):
            print("Model application is %s percent complete" % (100 * float(i) / grid_ts.shape[1]))
            for j in range(grid_ts.shape[2]):
                y = grid_ts[:, i, j]
                not_nans = (y != 0)  # Assume nans are 0's since using int
                pixel_obs = sum(not_nans)
                if pixel_obs >= 25:
                    X = X_template[not_nans]
                    y = y[not_nans]
                    # t = time[not_nans]
                    # Changed to this
                    t = times[not_nans]
                    # Run phenology regression with no change point
                    # Return complete data likelihood at maximum and posterior expectation
                    CDLL_pr = phenology_regression(X, y, 3, 3, 365, trace=False)
                    # Run change point regression;
                    # Return complete data likelihood at maximum and posterior expectation
                    CDLL_cp, ch_id = phenology_smooch(t, X, y, 3, 3, 365)
                    # Compute change point probability using marginal likelihoods
                    l = 1000  # this needs to be estimated using cross validation
                    cp_prob = CDLL_cp / CDLL_pr + l * logn
                    results[:, i, j] = [cp_prob, ch_id]
        end_time = clock.time()
        print("Regression model completed in {} seconds".format((end_time - start_time)))
        # Store the data
        print(results)
        np.save('results_%s_%s.npy' % (top_left[0], top_left[1]), results)
        return True

# TODO: Trace this class
class MockModel(Model):
    """
    Model created for testing
    """

    # TODO: Trace this function
    def apply_model(self, data, times, top_left) -> bool:
        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span("MockModel.apply_model"):
            grid_ts = data
            times = times
            top_left = top_left
            # Convert time to days since 01/01/1970
            start = date(1970, 1, 1)

            def str_to_date(a):
                return (datetime.strptime(a, '%Y%m%d').date())

            def date_to_days(a, start):
                return (a - start).days

            vstr_to_date = np.vectorize(str_to_date)
            vdate_to_days = np.vectorize(date_to_days)
            times = vstr_to_date(times)
            times = vdate_to_days(times, start)
            n = len(times)
            results = np.empty([2, grid_ts.shape[1], grid_ts.shape[2]]).astype('float16')
            for i in range(grid_ts.shape[1]):
                print("Model application is %s percent complete" % (100 * float(i) / grid_ts.shape[1]))
                for j in range(grid_ts.shape[2]):
                    y = grid_ts[:, i, j]
                    not_nans = (y != 0)
                    t = times[not_nans]
                    final_t = np.random.choice(t) if len(t) > 0 else 0
                    results[:, i, j] = [random.uniform(0,1), final_t]
            return True
