### Background & Goal of Project: 
About same amount users experience 30-50 (Medium increase group) v.s. 50-100 (High increase group) score increase.

Study the key difference between the two groups (in terms of their sign-up profiles and actions they took) and make recommendations to Medium increase group so that they can be moved to High increase group.

### Two groups of users:
- Medium increase group: Users who experienced 30-50 credit score increase

- High increase group: Users who experienced 50-100 credit score increase

### Data that sql.sql pulls:

Profile dimensions: 
  - initial credit score
  - #of different types tradelines they have
  - #of negative marks
  - credit utilization

Actions: 
  - Change of tradelines (open/close tradelines)?
  - Removed negative marks?
  - Credit utilization change
  - Account mix grade changed?
  - Account age grade changed?
  
  ### Methodology
  1. Feature Selection
  with two groups labelled:
  Meidum group: 0
  High group: 1
