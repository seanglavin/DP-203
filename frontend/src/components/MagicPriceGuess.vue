<template>
  <div class="magic-price-guess-container p-6 max-w-lg mx-auto bg-[var(--secondary-light)] rounded-xl shadow-md">
    <h2 class="text-2xl font-bold text-center text-[var(--primary-light)] bg-[var(--primary-purple)] py-3 px-4 -mx-6 -mt-6 mb-6">
      Magic Card Price Guesser
    </h2>

    <!-- Loading State -->
    <div v-if="gameState === 'loading'" class="text-center py-10">
      <p class="text-gray-500 mb-2">Loading game data...</p>
      <div class="card-image-container mb-4 h-80 flex justify-center items-center bg-gray-800 rounded-lg overflow-hidden mx-auto" style="max-width: 240px;">
         <img
            src="../assets/Back_of_MTG_card.png"
            alt="Loading card..."
            class="max-w-full max-h-full object-contain opacity-50"
          />
      </div>
      <div class="w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto"></div>
    </div>

    <!-- Error State -->
    <div v-else-if="gameState === 'error'" class="text-center text-red-500 p-4 bg-red-100 rounded">
      <p class="font-semibold">Error loading game:</p>
      <p class="text-sm">{{ error }}</p>
      <button @click="initializeGame" class="mt-3 py-1 px-3 bg-red-500 text-white rounded hover:bg-red-600 text-sm">
        Retry
      </button>
    </div>

    <!-- Game Area -->
    <div v-else class="game-area">
      <!-- Round/Score Display -->
      <div class="flex justify-between items-center mb-4 text-sm text-gray-600">
        <span>Round: {{ currentRound + 1 }} / {{ totalRounds }}</span>
        <span>Total Score: {{ totalScore }}</span>
      </div>

      <!-- Card Display (Guessing and Result States) -->
      <div v-if="gameState === 'guessing' || gameState === 'result'" class="card-display mb-6">
         <div class="card-image-container mb-4 h-80 flex justify-center items-center bg-gray-800 rounded-lg overflow-hidden">
            <img
                v-if="currentGameCard.image_uris && currentGameCard.image_uris.normal"
                :src="currentGameCard.image_uris.normal"
                :alt="currentGameCard.name"
                class="max-w-full max-h-full object-contain"
            />
            <div v-else class="text-gray-400">No Image Available</div>
        </div>
        <h3 class="text-xl font-bold text-center mb-4">{{ currentGameCard.name }}</h3>
      </div>

      <!-- Guessing State -->
      <div v-if="gameState === 'guessing'" class="guessing-controls">
        <label for="price-slider" class="block text-center font-semibold mb-2">Guess the Price (USD):</label>
        <input
          type="range"
          id="price-slider"
          min="0"
          :max="sliderMax"
          step="0.01"
          v-model="userGuess"
          class="w-full mb-2 accent-[var(--primary-purple)]"
        />
        <p class="text-center text-2xl font-bold mb-6">${{ parseFloat(userGuess).toFixed(2) }}</p>
        <button
          @click="submitGuess"
          class="w-full py-2 px-6 bg-blue-500 text-white rounded hover:bg-blue-600"
        >
          Submit Guess
        </button>
      </div>

      <!-- Result State -->
      <div v-if="gameState === 'result'" class="result-display text-center">
        <p class="mb-1">Your Guess: <span class="font-semibold">${{ parseFloat(userGuess).toFixed(2) }}</span></p>
        <p class="mb-1">Actual Price: <span class="font-semibold">${{ actualPrice.toFixed(2) }}</span></p>
        <p class="text-lg font-bold mt-3" :class="roundScore > 0 ? 'text-green-600' : 'text-red-600'">
          Round Score: {{ roundScore }}
        </p>
        <button
          @click="nextRound"
          class="mt-6 w-full py-2 px-6 bg-green-500 text-white rounded hover:bg-green-600"
        >
          {{ currentRound < totalRounds - 1 ? 'Next Card' : 'Show Final Score' }}
        </button>
      </div>

      <!-- Game Over State -->
      <div v-if="gameState === 'gameOver'" class="game-over text-center">
        <h3 class="text-2xl font-bold mb-4">Game Over!</h3>
        <p class="text-xl mb-6">Your Final Score: <span class="font-bold text-[var(--primary-purple)]">{{ totalScore }}</span></p>
        <button
          @click="startNewGame"
          class="py-2 px-6 bg-blue-500 text-white rounded hover:bg-blue-600"
        >
          Play Again
        </button>
      </div>

    </div>
  </div>
</template>

<script>
const TOTAL_ROUNDS = 5;
const MAX_SCORE_PER_ROUND = 1000;
const SLIDER_MAX_DEFAULT = 50.00; // Increased default max for potentially higher prices
const MIN_PRICE_FILTER = 1.00; // Minimum price to include a card

export default {
  name: 'MagicPriceGuesser', // Corrected component name
  data() {
    return {
      gameState: 'loading', // loading, fetching, guessing, result, gameOver, error
      error: null,
      allPricedCards: [], // Cards from source with USD price > MIN_PRICE_FILTER
      gameCards: [], // 5 cards selected for the current game
      currentRound: 0,
      totalRounds: TOTAL_ROUNDS,
      currentGameCard: null,
      userGuess: (SLIDER_MAX_DEFAULT / 2).toFixed(2), // Start slider in the middle
      actualPrice: 0,
      roundScore: 0,
      totalScore: 0,
      sliderMax: SLIDER_MAX_DEFAULT,
    };
  },
  mounted() {
    this.initializeGame();
  },
  methods: {
    initializeGame() {
      this.gameState = 'loading';
      this.error = null;
      this.fetchPricedCards();
    },
    async fetchPricedCards() {
      this.gameState = 'fetching'; // Indicate fetching state
      try {
        const response = await fetch('http://localhost:8000/api/mtg/scryfall/cards/daily1000');

        if (!response.ok) {
          const errorData = await response.json();
          throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        if (!data || !Array.isArray(data)) {
            throw new Error("Invalid data received from the card endpoint.");
        }
        console.log(`Fetched ${data.length} cards from core-expansion set.`);

        // --- Price Filter ---
        this.allPricedCards = data.filter(card => {
            const price = this.getUsdPrice(card);
            return price !== null && price > MIN_PRICE_FILTER;
        });
        console.log(`Filtered down to ${this.allPricedCards.length} cards with USD price > $${MIN_PRICE_FILTER.toFixed(2)}.`);
        // --- End Filter ---

        // --- Error Check ---
        if (this.allPricedCards.length < TOTAL_ROUNDS) {
          throw new Error(`Not enough cards with price > $${MIN_PRICE_FILTER.toFixed(2)} found (${this.allPricedCards.length}) to play ${TOTAL_ROUNDS} rounds.`);
        }
        // --- End Error Check ---

        this.startNewGame(); // Start the game if enough cards are found

      } catch (err) {
        console.error('Error fetching or filtering priced cards:', err);
        this.error = err.message || 'Failed to load card data.';
        this.gameState = 'error';
      }
    },
    startNewGame() {
      this.totalScore = 0;
      this.currentRound = 0;
      this.gameCards = this.getRandomSample(this.allPricedCards, TOTAL_ROUNDS);
      console.log("Starting new game with cards:", this.gameCards.map(c => `${c.name} ($${this.getUsdPrice(c).toFixed(2)})`)); // Log price too
      this.setupRound();
    },
    setupRound() {
      if (this.currentRound >= this.gameCards.length) {
          console.error("Attempted to setup round beyond available game cards.");
          this.gameState = 'gameOver'; // Should not happen ideally
          return;
      }
      this.currentGameCard = this.gameCards[this.currentRound];
      this.actualPrice = this.getUsdPrice(this.currentGameCard);
      this.roundScore = 0;

      // --- Adjust Slider Max Dynamically (Optional but Recommended) ---
      // Set slider max to be somewhat higher than the actual price to give range
      // Or keep it fixed if you prefer consistent difficulty
      this.sliderMax = Math.max(SLIDER_MAX_DEFAULT, Math.ceil(this.actualPrice * 1.5)); // Example: 1.5x actual price, rounded up
      // --- End Adjustment ---

      // Reset guess to middle of the *new* slider range
      this.userGuess = (this.sliderMax / 2).toFixed(2);

      this.gameState = 'guessing';
      console.log(`Round ${this.currentRound + 1}: Card ${this.currentGameCard.name}, Price ${this.actualPrice.toFixed(2)}, Slider Max ${this.sliderMax}`);
    },
    submitGuess() {
      if (this.gameState !== 'guessing') return;

      const guess = parseFloat(this.userGuess);
      this.roundScore = this.calculateScore(guess, this.actualPrice);
      this.totalScore += this.roundScore;
      this.gameState = 'result';
    },
    nextRound() {
      if (this.gameState !== 'result') return;

      this.currentRound++;
      if (this.currentRound < TOTAL_ROUNDS) {
        this.setupRound();
      } else {
        this.gameState = 'gameOver';
      }
    },
    calculateScore(guess, actual) {
        if (actual <= 0) return 0; // Should be less likely now with filter

        const difference = Math.abs(guess - actual);
        // Scoring logic remains the same, adjust penaltyFactor if needed
        const penaltyFactor = 50; // Adjust this to control score sensitivity
        let score = MAX_SCORE_PER_ROUND - (difference * penaltyFactor);

        return Math.max(0, Math.round(score));
    },
    getUsdPrice(card) {
      // Safely extracts and converts price string to number
      if (card && card.prices && card.prices.usd) {
        const price = parseFloat(card.prices.usd);
        return isNaN(price) ? null : price;
      }
      return null;
    },
    getRandomSample(arr, size) {
      // Fisher-Yates (Knuth) Shuffle
      const shuffled = arr.slice();
      let i = arr.length, temp, index;
      while (i--) {
        index = Math.floor((i + 1) * Math.random());
        temp = shuffled[index];
        shuffled[index] = shuffled[i];
        shuffled[i] = temp;
      }
      return shuffled.slice(0, size);
    }
  },
};
</script>

<style scoped>
/* Add any specific styles if needed */
.card-image-container img {
  /* Optional: Add subtle shadow or border */
}
</style>