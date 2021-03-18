#!/usr/bin/env python
import sys
import h5py

expected_datasets = {
    '/exchange/baselineErrFIT1': 2,
    '/exchange/baselineErrFIT2': 2,
    '/exchange/baselineFIT1': 2,
    '/exchange/baselineFIT2': 2,
    '/exchange/contrastErrFIT1': 2,
    '/exchange/contrastErrFIT2': 2,
    '/exchange/contrastFIT1': 2,
    '/exchange/contrastFIT2': 2,
    '/exchange/exponentErrFIT2': 2,
    '/exchange/exponentFIT2': 2,
    '/exchange/frameSum': 2,
    '/exchange/g2avgFIT1': 3,
    '/exchange/g2avgFIT2': 3,
    '/exchange/norm-0-g2': 2,
    '/exchange/norm-0-stderr': 2,
    '/exchange/partition-mean-partial': 2,
    '/exchange/partition-mean-total': 2,
    '/exchange/partition_norm_factor': 2,
    '/exchange/pixelSum': 2,
    '/exchange/tau': 2,
    '/exchange/tauErrFIT1': 2,
    '/exchange/tauErrFIT2': 2,
    '/exchange/tauFIT1': 2,
    '/exchange/tauFIT2': 2,
    '/exchange/timestamp_clock': 2,
    '/exchange/timestamp_tick': 2,
    '/hdf_metadata_version': 2,
    '/measurement/instrument/acquisition/angle': 2,
    '/measurement/instrument/acquisition/attenuation': 2,
    '/measurement/instrument/acquisition/beam_center_x': 2,
    '/measurement/instrument/acquisition/beam_center_y': 2,
    '/measurement/instrument/acquisition/beam_size_H': 2,
    '/measurement/instrument/acquisition/beam_size_V': 2,
    '/measurement/instrument/acquisition/ccdxspec': 2,
    '/measurement/instrument/acquisition/ccdzspec': 2,
    '/measurement/instrument/acquisition/compression': 0,
    '/measurement/instrument/acquisition/dark_begin': 2,
    '/measurement/instrument/acquisition/dark_end': 2,
    '/measurement/instrument/acquisition/data_begin': 2,
    '/measurement/instrument/acquisition/data_end': 2,
    '/measurement/instrument/acquisition/data_folder': 0,
    '/measurement/instrument/acquisition/datafilename': 0,
    '/measurement/instrument/acquisition/parent_folder': 0,
    '/measurement/instrument/acquisition/root_folder': 0,
    '/measurement/instrument/acquisition/specfile': 0,
    '/measurement/instrument/acquisition/specscan_dark_number': 2,
    '/measurement/instrument/acquisition/specscan_data_number': 2,
    '/measurement/instrument/acquisition/stage_x': 2,
    '/measurement/instrument/acquisition/stage_z': 2,
    '/measurement/instrument/acquisition/stage_zero_x': 2,
    '/measurement/instrument/acquisition/stage_zero_z': 2,
    '/measurement/instrument/acquisition/xspec': 2,
    '/measurement/instrument/acquisition/zspec': 2,
    '/measurement/instrument/detector/adu_per_photon': 2,
    '/measurement/instrument/detector/bit_depth': 2,
    '/measurement/instrument/detector/blemish_enabled': 0,
    '/measurement/instrument/detector/distance': 2,
    '/measurement/instrument/detector/efficiency': 2,
    '/measurement/instrument/detector/exposure_period': 2,
    '/measurement/instrument/detector/exposure_time': 2,
    '/measurement/instrument/detector/flatfield': 2,
    '/measurement/instrument/detector/flatfield_enabled': 0,
    '/measurement/instrument/detector/gain': 2,
    '/measurement/instrument/detector/geometry': 0,
    '/measurement/instrument/detector/kinetics/first_usable_window': 2,
    '/measurement/instrument/detector/kinetics/last_usable_window': 2,
    '/measurement/instrument/detector/kinetics/top': 2,
    '/measurement/instrument/detector/kinetics/window_size': 2,
    '/measurement/instrument/detector/kinetics_enabled': 0,
    '/measurement/instrument/detector/lld': 2,
    '/measurement/instrument/detector/manufacturer': 0,
    '/measurement/instrument/detector/roi/x1': 2,
    '/measurement/instrument/detector/roi/x2': 2,
    '/measurement/instrument/detector/roi/y1': 2,
    '/measurement/instrument/detector/roi/y2': 2,
    '/measurement/instrument/detector/sigma': 2,
    '/measurement/instrument/detector/x_binning': 2,
    '/measurement/instrument/detector/x_dimension': 2,
    '/measurement/instrument/detector/x_pixel_size': 2,
    '/measurement/instrument/detector/y_binning': 2,
    '/measurement/instrument/detector/y_dimension': 2,
    '/measurement/instrument/detector/y_pixel_size': 2,
    '/measurement/instrument/source_begin/beam_intensity_incident': 2,
    '/measurement/instrument/source_begin/beam_intensity_transmitted': 2,
    '/measurement/instrument/source_begin/current': 2,
    '/measurement/instrument/source_begin/datetime': 0,
    '/measurement/instrument/source_begin/energy': 2,
    '/measurement/instrument/source_end/current': 2,
    '/measurement/instrument/source_end/datetime': 0,
    '/measurement/sample/orientation': 2,
    '/measurement/sample/temperature_A': 2,
    '/measurement/sample/temperature_A_set': 2,
    '/measurement/sample/temperature_B': 2,
    '/measurement/sample/temperature_B_set': 2,
    '/measurement/sample/thickness': 2,
    '/measurement/sample/translation': 2,
    '/measurement/sample/translation_table': 2,
    '/xpcs/Version': 0,
    '/xpcs/analysis_type': 0,
    '/xpcs/avg_frames': 2,
    '/xpcs/batches': 2,
    '/xpcs/blemish_enabled': 0,
    '/xpcs/compression': 0,
    '/xpcs/dark_begin': 2,
    '/xpcs/dark_begin_todo': 2,
    '/xpcs/dark_end': 2,
    '/xpcs/dark_end_todo': 2,
    '/xpcs/data_begin': 2,
    '/xpcs/data_begin_todo': 2,
    '/xpcs/data_end': 2,
    '/xpcs/data_end_todo': 2,
    '/xpcs/delays_per_level': 2,
    '/xpcs/dnophi': 2,
    '/xpcs/dnoq': 2,
    '/xpcs/dphilist': 2,
    '/xpcs/dphispan': 2,
    '/xpcs/dqlist': 2,
    '/xpcs/dqmap': 2,
    '/xpcs/dqspan': 2,
    '/xpcs/dynamic_mean_window_size': 2,
    '/xpcs/flatfield_enabled': 0,
    '/xpcs/input_file_local': 0,
    '/xpcs/input_file_remote': 0,
    '/xpcs/kinetics': 0,
    '/xpcs/lld': 2,
    '/xpcs/mask': 2,
    '/xpcs/normalization_method': 0,
    '/xpcs/normalize_by_framesum': 2,
    '/xpcs/normalize_by_smoothed_img': 2,
    '/xpcs/num_g2partials': 2,
    '/xpcs/output_data': 0,
    '/xpcs/output_file_local': 0,
    '/xpcs/output_file_remote': 0,
    '/xpcs/qmap_hdf5_filename': 0,
    '/xpcs/qphi_bin_to_process': 2,
    '/xpcs/sigma': 2,
    '/xpcs/smoothing_method': 0,
    '/xpcs/snophi': 2,
    '/xpcs/snoq': 2,
    '/xpcs/specfile': 0,
    '/xpcs/specscan_dark_number': 2,
    '/xpcs/specscan_data_number': 2,
    '/xpcs/sphilist': 2,
    '/xpcs/sphispan': 2,
    '/xpcs/sqlist': 2,
    '/xpcs/sqmap': 2,
    '/xpcs/sqspan': 2,
    '/xpcs/static_mean_window_size': 2,
    '/xpcs/stride_frames': 2,
    '/xpcs/swbinX': 2,
    '/xpcs/swbinY': 2,
    '/xpcs/twotime2onetime_window_size': 2
}

def check_dataset(h5file, path, n_dims):
    try:
        dset = h5file[path]
    except:
        return False    
    return len(dset.shape) == n_dims

def qc_xpcs_hdf5(h5file):
    missing_datasets = []
    for path in expected_datasets:
        n_dims = expected_datasets[path]
        if not check_dataset(h5file, path, n_dims):
            missing_datasets.append(path)
    return missing_datasets


def check_hdf_dataset(h5filename):
    try:
        # print('Checking {}'.format(h5filename))
        x_h5_file = h5py.File(h5filename, 'r')
        bad_dsets = qc_xpcs_hdf5(x_h5_file)
        if len(bad_dsets):
            print('Bad or missing datasets in {}'.format(h5filename))
            for bad_dset in bad_dsets:
                print('{}'.format(bad_dset))
            return False
        return True
    except Exception as e:
        print(e)
        print('Failed QC: {}'.format(sys.argv[1]))
        return False


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: ./{} <xpcs HDF5 filename>'.format(sys.argv[0]))
    result = check_hdf_dataset(sys.argv[1])
    if result:
        print(f'Passed QC: {sys.argv[1]}')
