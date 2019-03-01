#!/usr/bin/python3

import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
import pandas as pd
import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import argparse

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Visualization tool for batch job scheduler.')
	parser.add_argument('-f', '--file', type=str, required=True, help='csv output file of job-mon')

	args = parser.parse_args()
	prog = parser.prog
	csv_file = args.file

	df = pd.read_csv(csv_file)
	df["creationTime"] = pd.to_datetime(df["creationTime"], format="%Y-%m-%d %H:%M:%S +0000 %Z")
	df["scheduledTime"] = pd.to_datetime(df["scheduledTime"], format="%Y-%m-%d %H:%M:%S +0000 %Z")
	df["completionTime"] = pd.to_datetime(df["completionTime"], format="%Y-%m-%d %H:%M:%S +0000 %Z")
	start = df["creationTime"].append(df["scheduledTime"]).append(df["completionTime"]).min()
	now = df["creationTime"].append(df["scheduledTime"]).append(df["completionTime"]).max()
	df["scheduled"] = ~df["scheduledTime"].isna()
	df.fillna(now, inplace=True)
	df["creationTime"] = df["creationTime"] - start
	df["scheduledTime"] = df["scheduledTime"] - start
	df["completionTime"] = df["completionTime"] - start
	df.sort_values(by=['creationTime'], inplace=True)
	df.reset_index(drop=True, inplace=True)

	header = ['Pending','Running (Fast)','Running (Slow)']
	dataset = [pd.to_numeric(df["creationTime"]), pd.to_numeric(df["scheduledTime"] - df["creationTime"]), df["isPreferedNodes"] * pd.to_numeric(df["completionTime"] - df["scheduledTime"]), ~df["isPreferedNodes"] * pd.to_numeric(df["completionTime"] - df["scheduledTime"])]

	matplotlib.rc('font', serif='Helvetica Neue')
	matplotlib.rc('text', usetex='false')
	matplotlib.rcParams.update({'font.size': 40})

	N = len(df)
	ind = np.arange(N)
	width = 0.5

	plt.gcf().set_size_inches(0.075 * (now - start).total_seconds(), 1.2 * N)

	b = plt.barh(ind, dataset[0], width, color = 'w')
	p = plt.barh(ind, dataset[1], width, left = dataset[0], color = 'xkcd:ivory', linewidth = 1, edgecolor = 'k')
	rf = plt.barh(ind, dataset[2], width, left = dataset[0] + dataset[1], color = 'xkcd:chartreuse', linewidth = 1, edgecolor = 'k')
	rs = plt.barh(ind, dataset[3], width, left = dataset[0] + dataset[1], color = 'xkcd:coral', linewidth = 1, edgecolor = 'k')

	labels = df["scheduledNodes"]
	rects = plt.gca().patches
	i = 0
	for rect, label in zip(rects, labels):
		if df["scheduled"][i]:
			plt.gca().text(dataset[0][i] + dataset[1][i] + dataset[2][i] / 2 + dataset[3][i] / 2 , rect.get_y() - rect.get_height() / 2, label, ha='center', va='center', fontsize = 12)
		i += 1
		
	def timeTicks(x, pos):
		d = datetime.timedelta(seconds = x / 10**9)
		return str(d)

	formatter = matplotlib.ticker.FuncFormatter(timeTicks)
	plt.gca().xaxis.set_major_formatter(formatter)
	xl, xr = plt.xlim()
	xs = (xr - xl) // 10**9 // 5 * 10**9
	plt.xticks(np.arange(xl, xr, xs), fontsize = 12, rotation = 45)
	plt.xlabel('Time (s)', fontsize = 20)
	plt.yticks(ind, df["jobName"], fontsize = 12)
	plt.ylabel('Job', fontsize = 20)
	plt.legend((p[0], rf[0], rs[0]), (header[0], header[1], header[2]), fontsize = 12, ncol = 4, framealpha = 0, fancybox = True)
	plt.gca().invert_yaxis()
	plt.grid(linestyle='--')
	plt.savefig('trace.pdf')


