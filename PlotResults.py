import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np
import statsmodels.graphics.gofplots as sm

file_path = "Data/test_result.csv"
df = pd.read_csv(file_path)

# Remove 10 W reference
mask = df["Reference Power [W]"] == 10
df = df[~mask]


x = np.array(df["Reference Power [W]"])
y = np.array(df["Shelly Power Measurement [W]"])
errors = y - x
# errors = np.nan_to_num(errors)

# Fit linear regression
x = x[:, np.newaxis]
coefficient, _, _, _ = np.linalg.lstsq(x, y)
points = np.arange(0, 1801)

plt.figure(layout="constrained")
sns.set_theme()
# plots for standard distribution
fig, ax = plt.subplots(1, 2, layout="constrained")
sns.set_theme()
sns.histplot(errors, kde=True, color="blue", ax=ax[0])
ax[0].set_xlabel("Error")
sm.ProbPlot(errors).qqplot(line="s", ax=ax[1])
figure = plt.gcf()
figure.set_size_inches(12, 8)
plt.savefig("Data/probability_plot.png", dpi=600)

plt.figure(layout="constrained")
colors = ["#4E2A69", "#1A8AA2"]
sns.set_theme()
sns.set_palette(sns.color_palette(colors))
sns.scatterplot(
    x=df["Reference Power [W]"],
    y=df["Shelly Power Measurement [W]"],
    label="Measured power",
)
plt.plot([], [])
sns.lineplot(x=range(1800), y=range(1800), label="True power")
# sns.lineplot(x=points, y=points * coefficient, label="Fitted line")
plt.xlabel("Reference power [W]")
plt.ylabel("Shelly EM power measurement [W]")
plt.title("Comparison of measured power of energy meters against reference power")
plt.legend()

figure = plt.gcf()
figure.set_size_inches(7, 4.5)
plt.savefig("Data/scatter_plot.png", dpi=750)

# Zoom in
plt.xlim(999.999, 1000.001)
plt.ylim(1013.5, 1014.5)
figure = plt.gcf()
figure.set_size_inches(9, 6)
plt.savefig("Data/zoomed_in_scatter_plot.png", dpi=600)

coefficient = coefficient[0]
print("Coefficient: ", coefficient)
print("Inaccuracy: ", (coefficient - 1) / 1 * 100, "%")
