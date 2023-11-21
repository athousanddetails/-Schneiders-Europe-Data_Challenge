# SE-Europe Data Challenge Template
Solution Proposal for EcoForecast: Revolutionizing Green Energy Surplus Prediction in Europe

➡️ Context:
With increasing digitalisation and the ever-growing reliance on data servers, the significance of sustainable computing is on the rise. Schneider Electric, a pioneer in digital transformation and energy management, brings you this innovative challenge to play your part in reducing the carbon footprint of the computing industry.

The task is simple, yet the implications are profound. With aim to predict which European country will have the highest surplus of green energy in the next hour. This information will be critical in making important decisions, such as optimizing computing tasks to use green energy effectively and, consequently, reducing CO2 emissions.

🎯 Objective:
The goal is to create a model capable of predicting the country (from a list of nine) that will have the most surplus of green energy in the next hour. For this task, we need to consider both the energy generation from renewable sources (wind, solar, geothermic, etc.), and the load (energy consumption). The surplus of green energy is considered to be the difference between the generated green energy and the consumed energy.

The countries to focus on are: Spain, UK, Germany, Denmark, Sweden, Hungary, Italy, Poland, and the Netherlands.

The solution must not only align with Schneider Electric's ethos but also go beyond its current offerings, presenting an unprecedented approach.

## Approach to the Problem 

The whole idea was to Resample the data in 1H Time Frames, and then interpolate (as per challenge rules) the time frames of a said hour. If a full hour frame was missing it was ignored. 
Duplicates were removed, and all NaN information was filled by 0 (as per challenge rules). 

As for processing, we created a new Feature called Label, which is basically the result of the substracion of the Energy Load minus the Green Energy availble at the said time. 
We applied this in a way that the Label result is a number that represents the country with the higher surplus of energy, which is what we want to have as target for prediction. 

We trained the data with an ARIMA model to predict the country with the most surplus of available green energy in the next hour. 

## Conclusions

The hard and key part was the resampling methodology to unsure the data was ok. 
We are not fond of interpolating or keeping NaN because in this case they don't represent the reality. From our point of view the Missing Information should've been discard for the prediction, but those were the rules. 
Other than that it was a very streamlined process and a very enjoyable and challenging problem to solve. 
