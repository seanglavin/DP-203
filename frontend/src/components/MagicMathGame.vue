<template>
    <div class="magic-math-game-container p-6 max-w-3xl mx-auto bg-[var(--secondary-light)] rounded-xl shadow-md">
      <h2 class="text-2xl font-bold text-center text-[var(--primary-light)] bg-[var(--primary-purple)] py-3 px-4 -mx-6 -mt-6 mb-6">
        Magic: The Gathering - Numbers Game
      </h2>
  
      <!-- Loading State -->
      <div v-if="loading" class="text-center py-10">
        <p class="text-gray-500 mb-2">Loading game data...</p>
        <div class="w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto"></div>
      </div>
  
      <!-- Error State -->
      <div v-else-if="error" class="text-center text-red-500 p-4 bg-red-100 rounded">
        <p class="font-semibold">Error loading game:</p>
        <p class="text-sm">{{ error }}</p>
        <button @click="initializeGame" class="mt-3 py-1 px-3 bg-red-500 text-white rounded hover:bg-red-600 text-sm">
          Retry
        </button>
      </div>
  
      <!-- Game Ready State -->
      <div v-else class="game-area">
        <!-- Target Number -->
        <div class="target-number text-center mb-6">
          <h3 class="text-lg font-semibold mb-2">Target Number:</h3>
          <p class="text-4xl font-bold text-[var(--primary-purple)]">{{ targetNumber }}</p>
        </div>
  
        <!-- Equation Structure & User Selection -->
        <div class="equation-structure text-center mb-8 p-4 bg-gray-100 rounded">
          <h3 class="text-lg font-semibold mb-3">Equation:</h3>
          <div class="flex justify-center items-center flex-wrap gap-x-2 gap-y-3 text-xl">
            <template v-for="(item, index) in equationStructure" :key="`eq-${index}`">
              <!-- Blank Spot - Filled or Empty -->
              <div
                v-if="item.type === 'blank'"
                class="blank-spot border-2 rounded px-3 py-1 min-w-[90px] text-center cursor-pointer"
                :class="{
                  'border-dashed border-gray-400': !userSelection[getBlankIndex(index)],
                  'border-solid border-blue-500 bg-blue-50': userSelection[getBlankIndex(index)],
                  'border-red-500 bg-red-50': selectionErrorIndex === getBlankIndex(index)
                }"
                @click="clearSelection(getBlankIndex(index))"
                :title="userSelection[getBlankIndex(index)] ? `Click to clear ${userSelection[getBlankIndex(index)].name}` : 'Select a card from the pool'"
              >
                <span class="text-sm text-gray-500 block">{{ item.stat }}</span>
                <span v-if="userSelection[getBlankIndex(index)]" class="text-blue-700 font-semibold">
                  {{ getStatValue(userSelection[getBlankIndex(index)], item.stat) ?? 'N/A' }}
                </span>
                <span v-else class="text-gray-400">?</span>
              </div>
              <!-- Operator -->
              <span v-else class="operator font-bold">{{ item.value }}</span>
            </template>
            <span class="font-bold mx-2">=</span>
            <!-- Calculated Result -->
            <span class="result font-bold" :class="{'text-green-600': calculatedResult !== null && calculatedResult === targetNumber, 'text-red-600': calculatedResult !== null && calculatedResult !== targetNumber}">
              {{ calculatedResult !== null ? calculatedResult : '?' }}
            </span>
          </div>
           <p v-if="selectionError" class="text-xs text-red-500 mt-2">{{ selectionError }}</p>
           <p v-else class="text-xs text-gray-500 mt-2">Select cards from the pool below to fill the blanks. Click a filled blank to clear.</p>
           <div v-if="calculationError" class="text-xs text-red-500 mt-2">Calculation Error: {{ calculationError }}</div>
           <div v-if="calculatedResult !== null" class="mt-3 text-center">
              <p class="font-semibold" :class="{'text-green-600': calculatedResult === targetNumber, 'text-red-600': calculatedResult !== targetNumber}">
                  {{ calculatedResult === targetNumber ? 'Perfect Match!' : `Result: ${calculatedResult} (Difference: ${Math.abs(targetNumber - calculatedResult)})` }}
              </p>
           </div>
        </div>
  
        <!-- Card Pool -->
        <div class="card-pool mb-6">
          <h3 class="text-lg font-semibold mb-3 text-center">Card Pool (Available: {{ availableCardPool.length }})</h3>
          <div v-if="availableCardPool.length > 0" class="grid grid-cols-3 sm:grid-cols-5 gap-2">
            <div
              v-for="card in availableCardPool"
              :key="card.id"
              class="card-item p-2 border rounded bg-white text-center text-sm cursor-pointer hover:bg-blue-100 transition"
              @click="selectCard(card)"
              :title="`Use ${card.name}`"
            >
              <p class="font-semibold truncate">{{ card.name }}</p>
              <p class="text-xs text-gray-600">CMC: {{ card.cmc ?? 'N/A' }}</p>
              <p class="text-xs text-gray-600">P: {{ card.power ?? 'N/A' }}</p>
              <p class="text-xs text-gray-600">T: {{ card.toughness ?? 'N/A' }}</p>
            </div>
          </div>
           <p v-else class="text-center text-gray-500 italic">All cards from pool selected.</p>
        </div>
  
        <!-- Controls -->
        <div class="controls text-center space-x-4">
           <button
              @click="calculateResultHandler"
              class="py-2 px-6 bg-blue-500 text-white rounded hover:bg-blue-600 disabled:opacity-50 disabled:cursor-not-allowed"
              :disabled="!isSelectionComplete || calculatedResult !== null"
            >
              Calculate
            </button>
          <button
            @click="setupNewGame"
            class="py-2 px-6 bg-green-500 text-white rounded hover:bg-green-600"
          >
            New Game
          </button>
        </div>
      </div>
    </div>
  </template>
  
  <script>
  import { evaluate } from 'mathjs';
  
  const VALID_NUMERIC_STATS = ['cmc', 'power', 'toughness'];
  const OPERATORS = ['+', '-', '*', '/'];
  
  export default {
    name: 'MagicMathGame',
    data() {
      return {
        loading: true,
        error: null,
        allCards: [],
        cardPool: [],
        targetNumber: 0,
        equationStructure: [],
        userSelection: [], // Holds the selected card object for each blank, or null
        calculatedResult: null,
        calculationError: null,
        selectionError: null, // Error message during card selection
        selectionErrorIndex: null, // Index of the blank causing the error
      };
    },
    computed: {
      // Cards from the pool that haven't been assigned to a blank yet
      availableCardPool() {
        const selectedIds = new Set(this.userSelection.filter(Boolean).map(card => card.id));
        return this.cardPool.filter(card => !selectedIds.has(card.id));
      },
      // Check if all blanks in userSelection are filled with cards
      isSelectionComplete() {
        return this.userSelection.every(card => card !== null);
      },
    },
    mounted() {
      this.initializeGame();
    },
    methods: {
      async initializeGame() {
        this.loading = true;
        this.error = null;
        this.calculatedResult = null;
        this.calculationError = null;
        this.selectionError = null;
        this.selectionErrorIndex = null;
        try {
          await this.fetchDailySubset();
          if (this.allCards.length >= 10) { // Ensure enough cards *before* setup
            this.setupNewGame();
          } else {
             // Throw error if not enough cards were fetched/filtered initially
             throw new Error(`Need at least 10 cards with usable stats, found ${this.allCards.length}. Try regenerating the daily subset.`);
          }
        } catch (err) {
          this.error = err.message || 'Failed to initialize game.';
          console.error("Initialization error:", err);
          // Clear game state on error
          this.cardPool = [];
          this.equationStructure = [];
          this.userSelection = [];
          this.targetNumber = 0;
        } finally {
          this.loading = false;
        }
      },
      async fetchDailySubset() {
        try {
          const response = await fetch('http://localhost:8000/api/mtg/scryfall/cards/merged_cards/daily100');
          if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
          }
          const data = await response.json();
          if (data && Array.isArray(data)) {
            console.log(`Fetched ${data.length} cards from daily subset.`);
            // Filter more robustly during fetch
            this.allCards = data.filter(card => this.hasAnyValidStat(card));
            console.log(`Filtered down to ${this.allCards.length} cards with potentially usable stats.`);
            if (this.allCards.length === 0) {
                throw new Error("No cards with usable numeric stats (cmc, power, toughness) found in the daily subset.");
            }
          } else {
            this.allCards = [];
            throw new Error("Invalid data received from daily subset endpoint.");
          }
        } catch (err) {
          console.error('Error fetching daily subset:', err);
          this.allCards = [];
          throw err;
        }
      },
       hasAnyValidStat(card) {
          // Check if at least one of the target stats is a valid number
          return VALID_NUMERIC_STATS.some(stat => this.getStatValue(card, stat) !== null);
      },
      setupNewGame() {
        this.error = null;
        this.calculatedResult = null;
        this.calculationError = null;
        this.selectionError = null;
        this.selectionErrorIndex = null;
  
        if (this.allCards.length < 10) {
            this.error = "Cannot start new game: Not enough usable cards loaded.";
            return;
        }
  
        this.cardPool = this.getRandomSample(this.allCards, 10);
        this.targetNumber = Math.floor(Math.random() * 900) + 101;
  
        const numBlanks = Math.floor(Math.random() * 4) + 2;
        this.equationStructure = [];
        for (let i = 0; i < numBlanks; i++) {
          const randomStat = VALID_NUMERIC_STATS[Math.floor(Math.random() * VALID_NUMERIC_STATS.length)];
          this.equationStructure.push({ type: 'blank', stat: randomStat });
          if (i < numBlanks - 1) {
            const randomOperator = OPERATORS[Math.floor(Math.random() * OPERATORS.length)];
            this.equationStructure.push({ type: 'operator', value: randomOperator });
          }
        }
        this.userSelection = Array(numBlanks).fill(null); // Reset selection array
        console.log("New game setup:", { target: this.targetNumber, pool: this.cardPool.length, eq: this.equationStructure });
      },
      getRandomSample(arr, size) {
        const shuffled = arr.slice();
        let i = arr.length, temp, index;
        while (i--) {
          index = Math.floor((i + 1) * Math.random());
          temp = shuffled[index];
          shuffled[index] = shuffled[i];
          shuffled[i] = temp;
        }
        return shuffled.slice(0, size);
      },
      // Find the index within the userSelection array corresponding to the overall equation index
      getBlankIndex(equationIndex) {
          let blankCount = 0;
          for(let i = 0; i <= equationIndex; i++) {
              if (this.equationStructure[i].type === 'blank') {
                  blankCount++;
              }
          }
          return blankCount - 1; // Return the 0-based index of the blank
      },
      selectCard(card) {
        this.selectionError = null; // Clear previous errors
        this.selectionErrorIndex = null;
        this.calculatedResult = null; // Clear result when selection changes
        this.calculationError = null;
  
        // Find the first empty slot in userSelection
        const blankIndex = this.userSelection.findIndex(slot => slot === null);
  
        if (blankIndex !== -1) {
          // Find the corresponding stat required for this blank
          const equationItem = this.equationStructure.find((item, index) => item.type === 'blank' && this.getBlankIndex(index) === blankIndex);
          if (!equationItem) return; // Should not happen
  
          const requiredStat = equationItem.stat;
          const statValue = this.getStatValue(card, requiredStat);
  
          // Validate if the card has a usable value for the required stat
          if (statValue === null) {
              this.selectionError = `Card '${card.name}' does not have a valid number for stat '${requiredStat}'.`;
              this.selectionErrorIndex = blankIndex; // Highlight the problematic blank
              console.warn(this.selectionError);
              return; // Prevent selection
          }
  
          // Assign the card to the blank slot
          this.userSelection[blankIndex] = card;
        } else {
          console.log("All blanks are filled.");
          // Optionally provide feedback that all slots are full
        }
      },
      clearSelection(blankIndex) {
          if (blankIndex >= 0 && blankIndex < this.userSelection.length) {
              this.userSelection[blankIndex] = null;
              this.calculatedResult = null; // Clear result when selection changes
              this.calculationError = null;
              this.selectionError = null; // Clear errors
              this.selectionErrorIndex = null;
          }
      },
      calculateResultHandler() {
        if (!this.isSelectionComplete) return;
  
        this.calculationError = null;
        let expression = '';
        let blankCounter = 0;
  
        try {
          for (const item of this.equationStructure) {
            if (item.type === 'blank') {
              const card = this.userSelection[blankCounter];
              const value = this.getStatValue(card, item.stat);
              if (value === null) {
                throw new Error(`Invalid stat value for ${item.stat} from card ${card?.name || 'Unknown'} at position ${blankCounter + 1}.`);
              }
              expression += ` ${value} `;
              blankCounter++;
            } else if (item.type === 'operator') {
              expression += ` ${item.value} `;
            }
          }
  
          console.log("Evaluating expression:", expression);
          const result = evaluate(expression.trim());
  
          // Handle non-numeric results from evaluate (e.g., Infinity)
          if (typeof result !== 'number' || !isFinite(result)) {
               throw new Error(`Calculation resulted in a non-finite number (${result}). Check for division by zero.`);
          }
  
          this.calculatedResult = Math.round(result); // Round to nearest integer, adjust as needed
  
        } catch (err) {
          console.error("Calculation error:", err);
          this.calculationError = err.message || "An error occurred during calculation.";
          this.calculatedResult = null;
        }
      },
      getStatValue(card, stat) {
        if (!card) return null;
        const value = card[stat];
        if (typeof value === 'number') {
          return value;
        }
        // Handle string stats like 'power' and 'toughness'
        if (typeof value === 'string') {
           // Check for simple numeric strings
           if (/^\d+$/.test(value)) {
              return parseInt(value, 10);
           }
           // Handle '*' - could return null, 0, or throw error depending on game rules
           if (value === '*') {
               // For this game, let's treat '*' as unusable
               return null;
           }
           // Handle 'X' - also unusable in this context
           if (value.toUpperCase() === 'X') {
               return null;
           }
           // Add handling for other non-numeric strings if necessary
        }
        return null; // Default to null if not a number or recognized numeric string
      }
    },
  };
  </script>
  
  <style scoped>
  .blank-spot {
    min-height: 50px; /* Ensure blanks have some height */
    transition: border-color 0.3s ease, background-color 0.3s ease;
  }
  .card-item {
      transition: background-color 0.2s ease-in-out;
  }
  /* Style for the result text */
  .result {
      transition: color 0.3s ease;
  }
  </style>