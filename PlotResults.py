import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

file_path = "Data/test_result.csv"
df = pd.read_csv(file_path)

plt.figure(layout="constrained")
plt.scatter(
    df["Reference Power [W]"],
    df["Shelly Power Measurement [W]"],
    color="red",
    label="Measured power",
)
plt.plot(range(1800), range(1800), label="True power")
plt.xlabel("Reference power [W]")
plt.ylabel("Shelly EM power measurement [W]")
plt.legend()
plt.grid()
figure = plt.gcf()
figure.set_size_inches(12, 8)
plt.savefig("Data/scatter_plot.png", dpi=600)
# Zoom in
plt.xlim(999.999, 1000.001)
plt.ylim(1013.5, 1014.5)
figure = plt.gcf()
figure.set_size_inches(12, 8)
plt.savefig("Data/zoomed_in_scatter_plot.png", dpi=600)
plt.show()
