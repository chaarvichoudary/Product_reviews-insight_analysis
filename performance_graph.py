# Import necessary libraries
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Set the style
plt.style.use('seaborn')

# Define the metrics
metrics = np.array([0.9547, 0.9678, 0.9592, 0.9634])
metrics_labels = ['Accuracy', 'Precision', 'Recall', 'F1 Score']

# Define custom colors for each bar
colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4']  # Professional but vibrant colors

# Create figure and axis with specified size
plt.figure(figsize=(12, 7))

# Create bar plot with different colors
bars = plt.bar(metrics_labels, metrics, width=0.5, color=colors)

# Customize the appearance
plt.title('Model Performance Metrics', fontsize=16, pad=20, fontweight='bold')
plt.ylabel('Score', fontsize=14)

# Add value labels on top of each bar
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height,
             f'{height:.4f}',
             ha='center', va='bottom',
             fontsize=12,
             fontweight='bold')

# Adjust y-axis limits to add some padding
plt.ylim(0.90, 1.00)

# Add grid
plt.grid(axis='y', linestyle=':', alpha=0.3)

# Customize spines
plt.gca().spines['top'].set_visible(False)
plt.gca().spines['right'].set_visible(False)

# Make x-axis labels larger
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)

# Add a subtle shadow effect to bars
for bar in bars:
    bar.set_edgecolor('none')
    bar.set_alpha(0.8)

# Add a background color
plt.gca().set_facecolor('#f8f9fa')

# Add a legend with color descriptions
legend_elements = [plt.Rectangle((0,0),1,1, facecolor=color, alpha=0.8) for color in colors]
plt.legend(legend_elements, metrics_labels, 
          loc='upper center', 
          bbox_to_anchor=(0.5, -0.15),
          ncol=4,
          fontsize=10)

# Adjust layout to prevent cutting off
plt.tight_layout()

# Show the plot
plt.show()

# Optional: Save the plot with high resolution
# plt.savefig('performance_metrics_colorful.png', dpi=300, bbox_inches='tight')
