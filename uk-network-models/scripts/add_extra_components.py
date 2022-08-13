# SPDX-FileCopyrightText: : 2017-2020 The PyPSA-Eur Authors
#
# SPDX-License-Identifier: MIT

# coding: utf-8
"""
Adds extra extendable components to the clustered and simplified network.

Relevant Settings
-----------------

.. code:: yaml

    costs:
        year:
        USD2013_to_EUR2013:
        dicountrate:
        emission_prices:

    electricity:
        max_hours:
        marginal_cost:
        capital_cost:
        extendable_carriers:
            StorageUnit:
            Store:

.. seealso::
    Documentation of the configuration file ``config.yaml`` at :ref:`costs_cf`,
    :ref:`electricity_cf`

Inputs
------

- ``data/costs.csv``: The database of cost assumptions for all included technologies for specific years from various sources; e.g. discount rate, lifetime, investment (CAPEX), fixed operation and maintenance (FOM), variable operation and maintenance (VOM), fuel costs, efficiency, carbon-dioxide intensity.

Outputs
-------

- ``networks/elec_s{simpl}_{clusters}_ec.nc``:


Description
-----------

The rule :mod:`add_extra_components` attaches additional extendable components to the clustered and simplified network. These can be configured in the ``config.yaml`` at ``electricity: extendable_carriers:``. It processes ``networks/elec_s{simpl}_{clusters}.nc`` to build ``networks/elec_s{simpl}_{clusters}_ec.nc``, which in contrast to the former (depending on the configuration) contain with **zero** initial capacity

- ``StorageUnits`` of carrier 'H2' and/or 'battery'. If this option is chosen, every bus is given an extendable ``StorageUnit`` of the corresponding carrier. The energy and power capacities are linked through a parameter that specifies the energy capacity as maximum hours at full dispatch power and is configured in ``electricity: max_hours:``. This linkage leads to one investment variable per storage unit. The default ``max_hours`` lead to long-term hydrogen and short-term battery storage units.

- ``Stores`` of carrier 'H2' and/or 'battery' in combination with ``Links``. If this option is chosen, the script adds extra buses with corresponding carrier where energy ``Stores`` are attached and which are connected to the corresponding power buses via two links, one each for charging and discharging. This leads to three investment variables for the energy capacity, charging and discharging capacity of the storage unit.
"""
import logging
from _helpers import configure_logging

import pypsa
import pandas as pd
import numpy as np

from add_electricity import (load_costs, add_nice_carrier_names,
                             _add_missing_carriers_from_costs)

idx = pd.IndexSlice

logger = logging.getLogger(__name__)


def attach_storageunits(n, costs, elec_opts):
    carriers = elec_opts['extendable_carriers']['StorageUnit']
    max_hours = elec_opts['max_hours']

    _add_missing_carriers_from_costs(n, costs, carriers)

    buses_i = n.buses.index

    ## Added cavern potential and converted to p_nom_max constraints for each bus
    caverns_df=pd.DataFrame({"name":['GB0 0', 'GB0 1', 'GB0 10', 'GB0 11', 'GB0 12',
              'GB0 13', 'GB0 14', 'GB0 15', 'GB0 16',
              'GB0 17', 'GB0 18', 'GB0 2', 'GB0 3', 'GB0 4',
              'GB0 5', 'GB0 6', 'GB0 7', 'GB0 8', 'GB0 9',
              'GB1 0'],
              "total":[0.00000000e+00,0.00000000e+00,1.68338079e+09,1.04292708e+09,2.16135667e+07,0.00000000e+00,0.00000000e+00,
              2.52809699e+08,0.00000000e+00,3.12102378e+06,0.00000000e+00,0.00000000e+00,1.69314524e+07,7.01762772e+07,
              0.00000000e+00,0.00000000e+00,0.00000000e+00,0.00000000e+00,0.00000000e+00,2.65959239e+08]})
    caverns_df = caverns_df.set_index("name")
    caverns_p_df=caverns_df/24

    lookup_store = {"H2": "electrolysis", "battery": "battery inverter", "CAES":"CAES Compressor", "LAES":"LAES Power","ETES":"ETES Power", "NaS":"NaS Inverter", "FeFlow": "FeFlow Inverter"}
    lookup_dispatch = {"H2": "fuel cell", "battery": "battery inverter", "CAES":"CAES Turbine", "LAES":"LAES Power", "ETES":"ETES Power", "NaS":"NaS Inverter", "FeFlow": "FeFlow Inverter"}
    
    ## Storage technologies added:

    for carrier in carriers: 

        if carrier == 'CAES':
               for j in range(19):
                      if caverns_p_df.loc['GB0 '+str(j)].values[0] != 0:
                            n.add("StorageUnit", 'GB0 '+str(j)+' CAES',
                                   bus='GB0 '+str(j),
                                   carrier=carrier,
                                   p_nom_extendable=True,
                                   p_nom_max=caverns_p_df.loc['GB0 '+str(j)].values[0],
                                   capital_cost=costs.at[carrier, 'capital_cost'],
                                   marginal_cost=costs.at[carrier, 'marginal_cost'],
                                   efficiency_store=costs.at[lookup_store[carrier], 'efficiency'],
                                   efficiency_dispatch=costs.at[lookup_dispatch[carrier], 'efficiency'],
                                   max_hours=max_hours[carrier],
                                   cyclic_state_of_charge=True)
               
               n.add("StorageUnit", 'GB1 0 CAES',
                     bus='GB1 0',
                     carrier=carrier,
                     p_nom_extendable=True,
                     p_nom_max=caverns_p_df.loc['GB1 0'].values[0],
                     capital_cost=costs.at[carrier, 'capital_cost'],
                     marginal_cost=costs.at[carrier, 'marginal_cost'],
                     efficiency_store=costs.at[lookup_store[carrier], 'efficiency'],
                     efficiency_dispatch=costs.at[lookup_dispatch[carrier], 'efficiency'],
                     max_hours=max_hours[carrier],
                     cyclic_state_of_charge=True)

        elif carrier == 'LAES':
               n.madd("StorageUnit", buses_i, ' ' + carrier,
                     bus=buses_i,
                     carrier=carrier,
                     p_nom_extendable=True,
                     capital_cost=costs.at[carrier, 'capital_cost'],
                     marginal_cost=costs.at[carrier, 'marginal_cost'],
                     standing_loss=costs.at['LAES Energy','standing_loss'],
                     efficiency_store=costs.at[lookup_store[carrier], 'efficiency'],
                     efficiency_dispatch=costs.at[lookup_dispatch[carrier], 'efficiency'],
                     max_hours=max_hours[carrier],
                     cyclic_state_of_charge=True)
       
        elif carrier == 'ETES':
               n.madd("StorageUnit", buses_i, ' ' + carrier,
                     bus=buses_i,
                     carrier=carrier,
                     p_nom_extendable=True,
                     capital_cost=costs.at[carrier, 'capital_cost'],
                     marginal_cost=costs.at[carrier, 'marginal_cost'],
                     standing_loss=costs.at['ETES Energy','standing_loss'],
                     efficiency_store=costs.at[lookup_store[carrier], 'efficiency'],
                     efficiency_dispatch=costs.at[lookup_dispatch[carrier], 'efficiency'],
                     max_hours=max_hours[carrier],
                     cyclic_state_of_charge=True)
              
        else:
              n.madd("StorageUnit", buses_i, ' ' + carrier,
                     bus=buses_i,
                     carrier=carrier,
                     p_nom_extendable=True,
                     capital_cost=costs.at[carrier, 'capital_cost'],
                     marginal_cost=costs.at[carrier, 'marginal_cost'],
                     efficiency_store=costs.at[lookup_store[carrier], 'efficiency'],
                     efficiency_dispatch=costs.at[lookup_dispatch[carrier], 'efficiency'],
                     max_hours=max_hours[carrier],
                     cyclic_state_of_charge=True)


def attach_stores(n, costs, elec_opts):
    carriers = elec_opts['extendable_carriers']['Store']

    _add_missing_carriers_from_costs(n, costs, carriers)

    buses_i = n.buses.index
    bus_sub_dict = {k: n.buses[k].values for k in ['x', 'y', 'country']}

    if 'H2' in carriers:
        h2_buses_i = n.madd("Bus", buses_i + " H2", carrier="H2", **bus_sub_dict)

        n.madd("Store", h2_buses_i,
               bus=h2_buses_i,
               carrier='H2',
               e_nom_extendable=True,
               e_cyclic=True,
               capital_cost=costs.at["hydrogen storage", "capital_cost"])

        n.madd("Link", h2_buses_i + " Electrolysis",
               bus0=buses_i,
               bus1=h2_buses_i,
               carrier='H2 electrolysis',
               p_nom_extendable=True,
               efficiency=costs.at["electrolysis", "efficiency"],
               capital_cost=costs.at["electrolysis", "capital_cost"],
               marginal_cost=costs.at["electrolysis", "marginal_cost"])

        n.madd("Link", h2_buses_i + " Fuel Cell",
               bus0=h2_buses_i,
               bus1=buses_i,
               carrier='H2 fuel cell',
               p_nom_extendable=True,
               efficiency=costs.at["fuel cell", "efficiency"],
               #NB: fixed cost is per MWel
               capital_cost=costs.at["fuel cell", "capital_cost"] * costs.at["fuel cell", "efficiency"],
               marginal_cost=costs.at["fuel cell", "marginal_cost"])

    if 'battery' in carriers:
        b_buses_i = n.madd("Bus", buses_i + " battery", carrier="battery", **bus_sub_dict)

        n.madd("Store", b_buses_i,
               bus=b_buses_i,
               carrier='battery',
               e_cyclic=True,
               e_nom_extendable=True,
               capital_cost=costs.at['battery storage', 'capital_cost'],
               marginal_cost=costs.at["battery", "marginal_cost"])

        n.madd("Link", b_buses_i + " charger",
               bus0=buses_i,
               bus1=b_buses_i,
               carrier='battery charger',
               efficiency=costs.at['battery inverter', 'efficiency'],
               capital_cost=costs.at['battery inverter', 'capital_cost'],
               p_nom_extendable=True,
               marginal_cost=costs.at["battery inverter", "marginal_cost"])

        n.madd("Link", b_buses_i + " discharger",
               bus0=b_buses_i,
               bus1=buses_i,
               carrier='battery discharger',
               efficiency=costs.at['battery inverter','efficiency'],
               p_nom_extendable=True,
               marginal_cost=costs.at["battery inverter", "marginal_cost"])

    #####
    # NEW STORAGE TECHNOLOGIES 

    if 'CAES' in carriers:
        c_buses_i = n.madd("Bus", buses_i + " CAES", carrier="CAES", **bus_sub_dict)

        caverns_df=pd.DataFrame({"name":['GB0 0 CAES', 'GB0 1 CAES', 'GB0 10 CAES', 'GB0 11 CAES', 'GB0 12 CAES',
              'GB0 13 CAES', 'GB0 14 CAES', 'GB0 15 CAES', 'GB0 16 CAES',
              'GB0 17 CAES', 'GB0 18 CAES', 'GB0 2 CAES', 'GB0 3 CAES', 'GB0 4 CAES',
              'GB0 5 CAES', 'GB0 6 CAES', 'GB0 7 CAES', 'GB0 8 CAES', 'GB0 9 CAES',
              'GB1 0 CAES'],
              "total":[0.00000000e+00,0.00000000e+00,1.68338079e+09,1.04292708e+09,2.16135667e+07,0.00000000e+00,0.00000000e+00,
              2.52809699e+08,0.00000000e+00,3.12102378e+06,0.00000000e+00,0.00000000e+00,1.69314524e+07,7.01762772e+07,
              0.00000000e+00,0.00000000e+00,0.00000000e+00,0.00000000e+00,0.00000000e+00,2.65959239e+08]})
        caverns_df = caverns_df.set_index("name")

        for i in range(19):
              n.add("Store", "GB0 " + str(i) +" CAES",
                     bus="GB0 " + str(i) + " CAES",
                     carrier='CAES',
                     e_nom_extendable=True,
                     e_cyclic=True,
                     e_nom_max=caverns_df.loc["GB0 " + str(i) + " CAES"].values[0],
                     capital_cost=costs.at["CAES Storage", "capital_cost"],
                     marginal_cost=costs.at["CAES", "marginal_cost"])
        
        n.add("Store", "GB1 0 CAES",
               bus="GB1 0 CAES",
               carrier='CAES',
               e_nom_extendable=True,
               e_cyclic=True,
               e_nom_max=caverns_df.loc["GB1 0 CAES"].values[0],
               capital_cost=costs.at["CAES Storage", "capital_cost"],
               marginal_cost=costs.at["CAES", "marginal_cost"])

        n.madd("Link", c_buses_i + " Compressor",
               bus0=buses_i,
               bus1=c_buses_i,
               carrier='CAES compressor',
               p_nom_extendable=True,
               efficiency=costs.at["CAES Compressor", "efficiency"],
               capital_cost=costs.at["CAES Compressor", "capital_cost"],
               marginal_cost=costs.at["CAES Compressor", "marginal_cost"])

        n.madd("Link", c_buses_i + " Turbine",
               bus0=c_buses_i,
               bus1=buses_i,
               carrier='CAES turbine',
               p_nom_extendable=True,
               efficiency=costs.at["CAES Turbine", "efficiency"],
               capital_cost=costs.at["CAES Turbine", "capital_cost"] * costs.at["CAES Turbine", "efficiency"],
               marginal_cost=costs.at["CAES Turbine", "marginal_cost"])

    if 'LAES' in carriers:
        l_buses_i = n.madd("Bus", buses_i + " LAES", carrier="LAES", **bus_sub_dict)

        n.madd("Store", l_buses_i,
               bus=l_buses_i,
               carrier='LAES',
               e_nom_extendable=True,
               e_cyclic=True,
               capital_cost=costs.at["LAES Energy", "capital_cost"],
               marginal_cost=costs.at["LAES", "marginal_cost"],
               standing_loss=costs.at["LAES Energy", "standing_loss"])

        n.madd("Link", l_buses_i + " Compressor",
               bus0=buses_i,
               bus1=l_buses_i,
               carrier='LAES compressor',
               p_nom_extendable=True,
               efficiency=costs.at["LAES Power", "efficiency"],
               capital_cost=costs.at["LAES Power", "capital_cost"]/2.,
               marginal_cost=costs.at["LAES Power", "marginal_cost"])

        n.madd("Link", l_buses_i + " Turbine",
               bus0=l_buses_i,
               bus1=buses_i,
               carrier='LAES turbine',
               p_nom_extendable=True,
               efficiency=costs.at["LAES Power", "efficiency"],
               capital_cost=(costs.at["LAES Power", "capital_cost"]/2.) * costs.at["LAES Power", "efficiency"],
               marginal_cost=costs.at["LAES Power", "marginal_cost"])

    if 'ETES' in carriers:
        e_buses_i = n.madd("Bus", buses_i + " ETES", carrier="ETES", **bus_sub_dict)

        n.madd("Store", e_buses_i,
               bus=e_buses_i,
               carrier='ETES',
               e_nom_extendable=True,
               e_cyclic=True,
               capital_cost=costs.at["ETES Energy", "capital_cost"],
               marginal_cost=costs.at["ETES", "marginal_cost"],
               standing_loss=costs.at["ETES Energy", "standing_loss"])

        n.madd("Link", e_buses_i + " Charger",
               bus0=buses_i,
               bus1=e_buses_i,
               carrier='ETES charger',
               p_nom_extendable=True,
               efficiency=costs.at["ETES Power", "efficiency"],
               capital_cost=costs.at["ETES Power", "capital_cost"]/2.,
               marginal_cost=costs.at["ETES Power", "marginal_cost"])

        n.madd("Link", e_buses_i + " Turbine",
               bus0=e_buses_i,
               bus1=buses_i,
               carrier='ETES turbine',
               p_nom_extendable=True,
               efficiency=costs.at["ETES Power", "efficiency"],
               capital_cost=(costs.at["ETES Power", "capital_cost"]/2.) * costs.at["ETES Power", "efficiency"],
               marginal_cost=costs.at["ETES Power", "marginal_cost"])

    if 'NaS' in carriers:
        n_buses_i = n.madd("Bus", buses_i + " NaS", carrier="NaS", **bus_sub_dict)

        n.madd("Store", n_buses_i,
               bus=n_buses_i,
               carrier='NaS',
               e_cyclic=True,
               e_nom_extendable=True,
               capital_cost=costs.at['NaS Energy', 'capital_cost'],
               marginal_cost=costs.at["NaS", "marginal_cost"])

        n.madd("Link", n_buses_i + " charger",
               bus0=buses_i,
               bus1=n_buses_i,
               carrier='NaS charger',
               efficiency=costs.at['NaS Inverter', 'efficiency'],
               capital_cost=costs.at['NaS Inverter', 'capital_cost'],
               p_nom_extendable=True,
               marginal_cost=costs.at["NaS Inverter", "marginal_cost"])

        n.madd("Link", n_buses_i + " discharger",
               bus0=n_buses_i,
               bus1=buses_i,
               carrier='NaS discharger',
               efficiency=costs.at['NaS Inverter','efficiency'],
               p_nom_extendable=True,
               marginal_cost=costs.at["NaS Inverter", "marginal_cost"])

    if 'FeFlow' in carriers:
        f_buses_i = n.madd("Bus", buses_i + " FeFlow", carrier="FeFlow", **bus_sub_dict)

        n.madd("Store", f_buses_i,
               bus=f_buses_i,
               carrier='FeFlow',
               e_cyclic=True,
               e_nom_extendable=True,
               capital_cost=costs.at['FeFlow Energy', 'capital_cost'],
               marginal_cost=costs.at["FeFlow", "marginal_cost"])

        n.madd("Link", f_buses_i + " charger",
               bus0=buses_i,
               bus1=f_buses_i,
               carrier='FeFlow charger',
               efficiency=costs.at['FeFlow Inverter', 'efficiency'],
               capital_cost=costs.at['FeFlow Inverter', 'capital_cost'],
               p_nom_extendable=True,
               marginal_cost=costs.at["FeFlow Inverter", "marginal_cost"])

        n.madd("Link", f_buses_i + " discharger",
               bus0=f_buses_i,
               bus1=buses_i,
               carrier='FeFlow discharger',
               efficiency=costs.at['FeFlow Inverter','efficiency'],
               p_nom_extendable=True,
               marginal_cost=costs.at["FeFlow Inverter", "marginal_cost"])
    
    #####

def attach_hydrogen_pipelines(n, costs, elec_opts):
    ext_carriers = elec_opts['extendable_carriers']
    as_stores = ext_carriers.get('Store', [])

    if 'H2 pipeline' not in ext_carriers.get('Link',[]): return

    assert 'H2' in as_stores, ("Attaching hydrogen pipelines requires hydrogen "
            "storage to be modelled as Store-Link-Bus combination. See "
            "`config.yaml` at `electricity: extendable_carriers: Store:`.")

    # determine bus pairs
    attrs = ["bus0","bus1","length"]
    candidates = pd.concat([n.lines[attrs], n.links.query('carrier=="DC"')[attrs]])\
                    .reset_index(drop=True)

    # remove bus pair duplicates regardless of order of bus0 and bus1
    h2_links = candidates[~pd.DataFrame(np.sort(candidates[['bus0', 'bus1']])).duplicated()]
    h2_links.index = h2_links.apply(lambda c: f"H2 pipeline {c.bus0}-{c.bus1}", axis=1)

    # add pipelines
    n.madd("Link",
           h2_links.index,
           bus0=h2_links.bus0.values + " H2",
           bus1=h2_links.bus1.values + " H2",
           p_min_pu=-1,
           p_nom_extendable=True,
           length=h2_links.length.values,
           capital_cost=costs.at['H2 pipeline','capital_cost']*h2_links.length,
           efficiency=costs.at['H2 pipeline','efficiency'],
           carrier="H2 pipeline")


if __name__ == "__main__":
    if 'snakemake' not in globals():
        from _helpers import mock_snakemake
        snakemake = mock_snakemake('add_extra_components', network='elec',
                                  simpl='', clusters=5)
    configure_logging(snakemake)

    n = pypsa.Network(snakemake.input.network)
    elec_config = snakemake.config['electricity']
    
    Nyears = n.snapshot_weightings.objective.sum() / 8760.
    costs = load_costs(snakemake.input.tech_costs, snakemake.config['costs'], elec_config, Nyears)

    attach_storageunits(n, costs, elec_config)
    attach_stores(n, costs, elec_config)
    attach_hydrogen_pipelines(n, costs, elec_config)

    add_nice_carrier_names(n, snakemake.config)

    n.export_to_netcdf(snakemake.output[0])
