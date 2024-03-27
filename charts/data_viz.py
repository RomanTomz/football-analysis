import os
import sys
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)


import matplotlib.pyplot as plt
plt.style.use(os.path.join(root_path, 'assets', 'rose-pine.mplstyle'))


def plot_high_level_stats(df, home_team, away_team):
    # Count the occurrences of each result
    results = df['FTR'].value_counts()

    # Mapping the results to more descriptive labels
    labels = {
        'H': f'{home_team} Wins',
        'A': f'{away_team} Wins',
        'D': 'Draws'
    }
    
    # Creating the bar chart
    plt.figure(figsize=(8, 6))
    plt.bar([labels[result] for result in results.index], results.values)
    plt.xlabel('Result')
    plt.ylabel('Counts')
    plt.title(f'{home_team} vs {away_team} - Match Results')
    plt.tight_layout()
    return plt

def plot_goals(df, home_team, away_team):
    # Total goals scored by the home team and away team
    home_goals = df[df['HomeTeam'] == home_team]['FTHG'].sum() + df[df['AwayTeam'] == home_team]['FTAG'].sum()
    away_goals = df[df['HomeTeam'] == away_team]['FTHG'].sum() + df[df['AwayTeam'] == away_team]['FTAG'].sum()

    # Creating the bar chart
    plt.figure(figsize=(8, 6))
    plt.bar([f'{home_team} Goals', f'{away_team} Goals'], [home_goals, away_goals])
    plt.xlabel('Team')
    plt.ylabel('Goals Scored')
    plt.title(f'{home_team} vs {away_team} - Goals Scored')
    plt.tight_layout()
    return plt
