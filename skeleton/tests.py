
import pytest
import numpy as np
import torch
from model import _CNN
from data_util import split_csv, brain_regions
from explain_pipeline import prepare_dataloaders, eval, create_SHAP_values 
DATA_FOLDER = '../data/datasets/ADNI3'

class TestOperators:

    def test_eval_model(self):
        # Load instances
        split_csv(DATA_FOLDER + '/ADNI3.csv', DATA_FOLDER)
        bg_dl, test_dl = prepare_dataloaders(DATA_FOLDER + '/bg.csv', DATA_FOLDER + '/test.csv')

        # Load model
        model = _CNN(fil_num=20, drop_rate=0.)
        state = torch.load(DATA_FOLDER + '/cnn_best.pth', map_location=torch.device('cpu'))
        model.load_state_dict(state['state_dict'])

        # Evaluate model on bg and test data
        bg_correct = eval(bg_dl, model)
        test_correct = eval(test_dl, model)
        assert sum(test_correct) == 5 and sum(bg_correct) == 13

    def test_create_SHAP(self):
        # NOTE (soren): can't verify SHAP values so just check that they are being created

        # Load instances
        split_csv(DATA_FOLDER + '/ADNI3.csv', DATA_FOLDER)
        bg_dl, test_dl = prepare_dataloaders(DATA_FOLDER + '/bg.csv', DATA_FOLDER + '/test.csv')

        # Load model
        model = _CNN(fil_num=20, drop_rate=0.)
        state = torch.load(DATA_FOLDER + '/cnn_best.pth', map_location=torch.device('cpu'))
        model.load_state_dict(state['state_dict'])

        # Create SHAP values
        test_shap_vals, bg_shap_vals = create_SHAP_values(model, bg_dl, test_dl, 1, None)

        # Check if sizing matches up with what we expect
        assert len(test_shap_vals) == 2 and len(bg_shap_vals) == 2
        x, path, y = next(iter(test_dl))
        assert [*np.squeeze(test_shap_vals[0]).shape] == [*np.squeeze(x[0]).shape]
        x, path, y = next(iter(bg_dl))
        assert [*np.squeeze(bg_shap_vals[0]).shape] == [*np.squeeze(x[0]).shape]