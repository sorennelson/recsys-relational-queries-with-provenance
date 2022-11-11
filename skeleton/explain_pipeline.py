import argparse
import matplotlib as plt
from matplotlib.colors import LinearSegmentedColormap
import nibabel as nib
import numpy as np
import pandas as pd
import os
import shap
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
from data_util import CNN_Data, split_csv, brain_regions
from model import _CNN


# This is a color map that you can use to plot the SHAP heatmap on the input MRI
colors = []
for l in np.linspace(1, 0, 100):
    colors.append((30./255, 136./255, 229./255,l))
for l in np.linspace(0, 1, 100):
    colors.append((255./255, 13./255, 87./255,l))
red_transparent_blue = LinearSegmentedColormap.from_list("red_transparent_blue", colors)


# Returns two data loaders (objects of the class: torch.utils.data.DataLoader) that are
# used to load the background and test datasets.
def prepare_dataloaders(bg_csv, test_csv, bg_batch_size = 8, test_batch_size= 1, num_workers=1):
    '''
    Attributes:
        bg_csv (str): The path to the background CSV file.
        test_csv (str): The path to the test data CSV file.
        bg_batch_size (int): The batch size of the background data loader
        test_batch_size (int): The batch size of the test data loader
        num_workers (int): The number of sub-processes to use for dataloader
    '''
    # YOUR CODE HERE
    bg_ds = CNN_Data(bg_csv)
    test_ds = CNN_Data(test_csv)
    
    # Create Dataloader
    bg_dl = DataLoader(bg_ds, bg_batch_size, shuffle=False, num_workers=num_workers)
    test_dl = DataLoader(test_ds, test_batch_size, shuffle=False, num_workers=num_workers)

    return bg_dl, test_dl

# Evaluates the model on the given dataset. Returns the number of correct predictions
def eval(loader, model):
    '''
    Attributes:
        loader (Torch DataLoader)
        model (Torch model):
    '''
    model.eval()
    correct = []

    for x, _, y in loader:
        # Add in channel dim
        x = x.unsqueeze(1)
        # Make predictions
        with torch.no_grad():
            pred = model(x)
        # Eval predictions
        correct.extend((torch.argmax(pred, -1) == y).tolist())

    return correct

# Merge batches from dataloader
def __create_SHAP_ds(loader, mri_count=None):
    '''
    Attributes:
        loader (torch.utils.data.DataLoader): Dataloader instance for the dataset.
        mri_count (int): number of instances wanted in SHAP dataset
    '''
    X = []
    for x, _, _ in loader:
        # Add in channel dim
        X.append(x.unsqueeze(1))
    X = torch.cat(X, 0)
    X = X[:mri_count] if mri_count else X
    return X

            
# Generates SHAP values for all pixels in the MRIs given by the test_loader
def create_SHAP_values(model, bg_loader, test_loader, mri_count, save_path):
    '''
    Attributes:
        bg_loader (torch.utils.data.DataLoader): Dataloader instance for the background dataset.
        test_loader (torch.utils.data.DataLoader): Dataloader instance for the test dataset.
        mri_count (int): The total number of explanations to generate.
        save_path (str): The path to save the generated SHAP values (as .npy files).
    '''
    # Create Tensor of background/test data
    full_bg = __create_SHAP_ds(bg_loader, min(mri_count, 19))
    bg = __create_SHAP_ds(bg_loader, min(mri_count, 5))
    test = __create_SHAP_ds(test_loader, min(mri_count, 5))

    # Generate shap values
    model.eval()
    de = shap.DeepExplainer(model, bg)
    test_shap_vals = de.shap_values(test)
    bg_shap_vals = de.shap_values(full_bg)
    return test_shap_vals, bg_shap_vals

# Save shap vals for correct predictions
def save_correct_SHAP_values(correct, shap_vals, loader, save_path):
    '''
    Attributes:
        correct ([bool]): Correct/Incorrect prediction corresponding to loader instances
        shap_vals ([np.array]): SHAP values corresponding to loader instances
        loader: (torch.utils.data.DataLoader): Dataloader instance for the dataset.
        save_path: path to output folder.
    '''
    global_idx = 0
    for _, path, y in loader:
        # Iterate over each batch instance
        for i in range(len(y)):
            if global_idx == len(shap_vals[0]):
                return

            if correct[i]:
                shap_val = shap_vals[y[i]][i]
                output_path = '{}/{}'.format(save_path, path[i].split('/')[-1])
                np.save(output_path, shap_val)
            
            global_idx += 1

# Aggregates SHAP values per brain region and returns a dictionary that maps 
# each region to the average SHAP value of its pixels. 
def aggregate_SHAP_values_per_region(shap_values, seg_path, brain_regions):
    '''
    Attributes:
        shap_values (ndarray): The shap values for an MRI (.npy).
        seg_path (str): The path to the segmented MRI (.nii). 
        brain_regions (dict): The regions inside the segmented MRI image (see data_utl.py)
    '''
    # YOUR CODE HERE

    seg = nib.load(seg_path).get_fdata()
    region_avg = {}
    for region in brain_regions.keys():
        mask = np.where(seg==region, 1, 0)
        region_avg[region] = np.sum(shap_values * mask) / np.sum(mask)
    return region_avg

# Averages across each instance per-region then sorts the averages.
# Output is [(region_number, region_name, avg)]
def compute_sorted_global_per_region_avg(region_avgs, brain_regions):
    '''
    Attributes:
        region_avgs ([dict]): list of per-region avgs for each instance  
        brain_regions (dict): The regions inside the segmented MRI image (see data_utl.py)
    '''
    global_avg = []
    # Compute per region avg across all samples
    for region, r_name in brain_regions.items():
        global_avg.append((
            region,
            r_name,
            np.average([avg[region] for avg in region_avgs])
        ))

    # Return avgs sorted by avg
    return sorted(global_avg, key=lambda x: x[2], reverse=True)

# Returns a list containing the top-10 most contributing brain regions to each predicted class (AD/NotAD).
def output_top_10_lst(csv_file):
    '''
    Attribute:
        csv_file (str): The path to a CSV file that contains the aggregated SHAP values per region.
    '''
    # YOUR CODE HERE
    df = pd.read_csv(csv_file)
    return list(df['region'])

 # Returns a path for a correct AD instance and a path for correct NotAD instance 
 # (or None if there aren't any)
def find_ad_notad_paths(correct, loader):
    '''
    Attributes:
        correct ([bool]): Correct/Incorrect prediction corresponding to loader instances
        loader: (torch.utils.data.DataLoader): Dataloader instance for the dataset.
    '''
    ad_path, notad_path = None, None
    for _, path, y in loader:
        # Iterate over each batch instance
        for i in range(len(y)):

            # Grab first correct AD instance and NotAD instance
            if correct[i]:
                if y[i] and not ad_path:
                    ad_path = path[i]
                elif not y[i] and not notad_path:
                    notad_path = path[i]
                
                if ad_path and notad_path:
                    return ad_path, notad_path
    
    return ad_path, notad_path

# Plots SHAP values on a 2D slice of the 3D MRI. 
def plot_shap_on_mri(subject_mri, shap_values):
    '''
    Attributes:
        subject_mri (str): The path to the MRI (.npy).
        shap_values (str): The path to the SHAP explanation that corresponds to the MRI (.npy).
    '''
    # YOUR CODE HERE
    mri = np.expand_dims(np.load(subject_mri), (0,-1))
    shap_vals = np.expand_dims(np.load(shap_values), -1)
    # 2D slice in middle of 3D mri
    midpoint = mri.shape[1] // 2
    shap.image_plot(shap_vals[:,midpoint,:,:,:], mri[:,midpoint,:,:,:], show=False)
    # Save plot
    fname = subject_mri.split('/')[-1].split('.')[0]
    path = '{}/SHAP/heatmaps/{}'.format(args.outputFolder, fname)
    plt.pyplot.savefig(path)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Assignement 3.')
    parser.add_argument('--task', type=int, help='Which task to run [1 / 2 / 3 / 4].')
    parser.add_argument('--dataFolder', type=str, default='../data/datasets/ADNI3', help='path to the ADNI3 folder.')
    parser.add_argument('--outputFolder', type=str, default='output',
                         help='path to the output folder where we will store the final outputs.')

    args = parser.parse_args()

    # Load instances
    split_csv(args.dataFolder + '/ADNI3.csv', args.dataFolder)
    bg_dl, test_dl = prepare_dataloaders(args.dataFolder + '/bg.csv', args.dataFolder + '/test.csv')

    # Load model
    model = _CNN(fil_num=20, drop_rate=0.)
    state = torch.load(args.dataFolder + '/cnn_best.pth', map_location=torch.device('cpu'))
    model.load_state_dict(state['state_dict'])


    # TASK I: Load CNN model and isntances (MRIs)
    #         Report how many of the 19 MRIs are classified correctly
    # YOUR CODE HERE 
    if args.task == 1:

        if not os.path.exists(args.outputFolder):
            os.makedirs(args.outputFolder)

        # Evaluate model on bg and test data
        correct = eval(bg_dl, model)
        test_correct = eval(test_dl, model)
        correct += test_correct

        df = pd.DataFrame([['Correct', sum(correct)], ['Incorrect', len(correct)]], columns=['Classified', 'Value'])
        df.to_csv('{}/task-1.csv'.format(args.outputFolder), index=False)
    

    # TASK II: Probe the CNN model to generate predictions and compute the SHAP 
    #          values for each MRI using the DeepExplainer or the GradientExplainer. 
    #          Save the generated SHAP values that correspond to instances with a
    #          correct prediction into output/SHAP/data/
    # YOUR CODE HERE 
    elif args.task == 2:

        output_path = '{}/SHAP'.format(args.outputFolder)
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        test_shap_vals, bg_shap_vals = create_SHAP_values(model, bg_dl, test_dl, 19, None)
        # Evaluate models to only save correct shap vals
        bg_correct = eval(bg_dl, model)
        test_correct = eval(test_dl, model)

        save_correct_SHAP_values(bg_correct, bg_shap_vals, bg_dl, output_path)
        save_correct_SHAP_values(test_correct, test_shap_vals, test_dl, output_path)

        
    # TASK III: Plot an explanation (pixel-based SHAP heatmaps) for a random MRI. 
    #           Save heatmaps into output/SHAP/heatmaps/
    # YOUR CODE HERE 
    elif args.task == 3:
        heatmap_path = '{}/SHAP/heatmaps'.format(args.outputFolder)
        if not os.path.exists(heatmap_path):
            os.makedirs(heatmap_path)

        test_correct = eval(test_dl, model)
        # Grab a path that corresponds to AD/Not AD
        ad_path, notad_path = find_ad_notad_paths(test_correct, test_dl)

        # Only 5 test instances so possibility of only one class, if so grab other path from bg
        if not ad_path or not notad_path:
            bg_correct = eval(bg_dl, model)
            bg_ad_path, bg_notad_path = find_ad_notad_paths(bg_correct, bg_dl)
            if not ad_path:
                ad_path = bg_ad_path
            if not notad_path:
                notad_path = bg_notad_path
        
        assert ad_path and notad_path
        plot_shap_on_mri(ad_path,
                        '{}/SHAP/{}'.format(args.outputFolder, ad_path.split('/')[-1]))
        plot_shap_on_mri(notad_path,
                        '{}/SHAP/{}'.format(args.outputFolder, notad_path.split('/')[-1]))


    # TASK IV: Map each SHAP value to its brain region and aggregate SHAP values per region.
    #          Report the top-10 most contributing regions per class (AD/NC) as top10_{class}.csv
    #          Save CSV files into output/top10/
    # YOUR CODE HERE 
    elif args.task == 4:

        output_path = '{}/top10'.format(args.outputFolder)
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        test_correct = eval(test_dl, model)

        ad_region_avgs, notad_region_avgs = [], []
        global_idx = 0
        for x, path, y in test_dl:
            for i in range(len(y)):
                # Ignore incorrect predictions
                if not test_correct[global_idx]:
                    continue

                # Load in shap_vals and grab seg_path
                fname = path[i].split('/')[-1]
                shap_vals = np.load('{}/SHAP/{}'.format(args.outputFolder, fname))
                seg_path = '{}/seg/{}.nii'.format(args.dataFolder, fname.split('.')[0])

                # Aggregate per region shap vals for this image and add to corresponding class list
                region_avg = aggregate_SHAP_values_per_region(shap_vals, seg_path, brain_regions)
                if y[i]:
                    ad_region_avgs += [region_avg]
                else:
                    notad_region_avgs += [region_avg]
                
                global_idx += 1

        # Compute global avgs
        ad_global_region_avg = compute_sorted_global_per_region_avg(ad_region_avgs, brain_regions)[:10]
        notad_global_region_avg = compute_sorted_global_per_region_avg(notad_region_avgs, brain_regions)[:10]

        # Output
        ad_df = pd.DataFrame(ad_global_region_avg, columns=['Region number', 'region', 'value'])
        ad_df.to_csv('{}/task-4-true.csv'.format(output_path), index=False)
        notad_df = pd.DataFrame(notad_global_region_avg, columns=['Region number', 'region', 'value'])
        notad_df.to_csv('{}/task-4-false.csv'.format(output_path), index=False)
