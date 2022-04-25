/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

let qualReportSummary = {
    "config": {
        "showTLCSummary": false
    },
    "totalApps": {
        "numeric": 0,
        "header": "Total Applications",
        "statsPercentage": "%",
        "statsTimeFrame": "Apps with Estimated End Time",
        "totalAppsDurations": "0 ms",
        "totalAppsDurationLabel": "Total Run Durations",
    },
    "candidates": {
        "numeric": 0,
        "header": "RAPIDS Candidates",
        "statsPercentage": "%",
        "statsTimeFrame": "Fit for GPU acceleration",
    },
    "speedups": {
        "numeric": "N/A",
        "header": "Speedups",
        "statsPercentage": "% Expected GPU Accelration",
        "statsTimeFrame": "of Total Durations",
        "totalSqlDataframeTaskDuration" : "0 ms",
        "totalSqlDFDurationsLabel" : "Total SqlDF Durations",
    },
    "tlc": {
        "numeric": 0,
        "header": "Apps that need TLC",
        "statsPercentage": "% Needs more information",
        "statsTimeFrame": "We found apps with potential problems",
    },
};

let toolTipsValues = {
    "gpuRecommendations": {
        "App Name": "Name of the application",
        "App ID": "An application is referenced by its application ID, [app-id]. \
              When running on YARN, each application may have multiple attempts, but there are attempt IDs only for applications in cluster mode, not applications in client mode. Applications in YARN cluster mode can be identified by their [attempt-id].",
        "App Duration": "Wall-Clock time measured since the application starts till it is completed. If an app is not completed an estimated completion time would be computed.",
        "Opportunity": "Expected percentage of the speedup after adding RAPIDS plugin",
        "Recommendation": "On a scale from 1 to 5, is it recommended to use RAPIDS"
    }
}

let UIConfig = {
    "datatables.gpuRecommendations": {
        "rowgroup.enabled": false,
        "searchPanes": {
            enabled: true,
            "dtConfigurations": {
                initCollapsed: false,
                viewTotal: true,
                // Note that there is a bug in cascading that breaks paging of the table
                cascadePanes: true,
                show: false,
            },
            "panes": {
                "recommendation": {
                    "header": "Gpu Recommendations",
                    "search": true,
                    "order": [[0, 'desc']],
                },
                "users":{
                    "header": "Spark User",
                    "search": true,
                }
            }
        },
        "Dom" : {
            default: 'frtlip',
        },
        "buttons": {
            enabled: true,
            buttons: [
                {
                    extend: 'csv',
                    text: 'Export'
                }
            ],
        }
    }
}
