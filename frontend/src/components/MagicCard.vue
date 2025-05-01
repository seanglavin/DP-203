<template>
    <div class="magic-card-container p-6 max-w-md mx-auto bg-[var(--secondary-light)] rounded-xl shadow-md overflow-hidden">
      <h2 class="text-2xl font-bold text-center text-[var(--primary-light)] bg-[var(--primary-purple)] py-3 px-4 -mx-6 -mt-6 mb-6">
        Random Magic Card
      </h2>
  
      <div v-if="loading" class="flex justify-center items-center h-96">
        <div class="text-center">
          <p class="text-gray-500 mb-2">Loading today's cards...</p>
          <div class="w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto"></div>
        </div>
      </div>
  
      <div v-else-if="error" class="text-center text-red-500 p-4 bg-red-100 rounded">
        <p>Error loading cards:</p>
        <p>{{ error }}</p>
        <button @click="fetchDailySubset" class="mt-2 py-1 px-3 bg-red-500 text-white rounded hover:bg-red-600">
          Retry
        </button>
      </div>
  
      <div v-else-if="currentCard" class="card-display">
        <!-- Card Image -->
        <div class="card-image-container mb-4 h-80 flex justify-center items-center bg-gray-800 rounded-lg overflow-hidden">
            <img
                v-if="currentCard.image_uris && currentCard.image_uris.normal"
                :src="currentCard.image_uris.normal"
                :alt="currentCard.name"
                class="max-w-full max-h-full object-contain"
            />
          <div v-else class="text-gray-400">No Image Available</div>
        </div>
  
        <!-- Card Details -->
        <div class="card-details p-4 bg-gray-100 rounded-lg mb-6">
          <h3 class="text-xl font-bold mb-2">{{ currentCard.name }}</h3>
          <p v-if="currentCard.mana_cost" class="mb-1"><span class="font-semibold">Cost:</span> {{ currentCard.mana_cost }}</p>
          <p class="mb-1"><span class="font-semibold">Type:</span> {{ currentCard.type_line }}</p>
          <p v-if="currentCard.power || currentCard.toughness" class="mb-1">
            <span class="font-semibold">P/T:</span>
            {{ currentCard.power ?? '*' }}/{{ currentCard.toughness ?? '*' }}
          </p>
           <p v-if="currentCard.loyalty" class="mb-1">
            <span class="font-semibold">Loyalty:</span> {{ currentCard.loyalty }}
          </p>
          <p v-if="currentCard.oracle_text" class="text-sm mt-2 italic">
            {{ currentCard.oracle_text }}
          </p>
           <p v-if="currentCard.flavor_text" class="text-sm mt-2 text-gray-600 italic">
            {{ currentCard.flavor_text }}
          </p>
          <p v-if="currentCard.artist" class="text-xs mt-2 text-right text-gray-500">
            Illus. {{ currentCard.artist }}
          </p>
        </div>
  
        <!-- Next Card Button -->
        <div class="text-center">
          <button
            @click="showRandomCard"
            class="py-2 px-8 bg-green-500 text-white rounded hover:bg-green-600 transition w-full"
          >
            Next Card
          </button>
        </div>
  
      </div>
  
       <div v-else class="text-center text-gray-500 p-4">
          No cards available in today's subset. Please try generating the daily file.
       </div>
  
    </div>
  </template>
  
  <script>
  export default {
    name: 'MagicCardDisplay',
    data() {
      return {
        loading: true,
        error: null,
        allCards: [], // Stores the fetched daily subset
        currentCard: null,
      };
    },
    mounted() {
      this.fetchDailySubset();
    },
    methods: {
      async fetchDailySubset() {
        this.loading = true;
        this.error = null;
        this.currentCard = null; // Clear current card while fetching
        this.allCards = [];
  
        try {
          // Adjust the endpoint if your API prefix is different
          const response = await fetch('http://localhost:8000/api/mtg/scryfall/cards/daily100');
  
          if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
          }
  
          const data = await response.json();
  
          if (data && Array.isArray(data) && data.length > 0) {
            this.allCards = data;
            this.showRandomCard(); // Show the first random card
          } else {
             this.error = "No cards found in today's subset.";
          }
        } catch (err) {
          console.error('Error fetching daily subset:', err);
          this.error = err.message || 'Failed to load card data.';
        } finally {
          this.loading = false;
        }
      },
      showRandomCard() {
        if (!this.allCards || this.allCards.length === 0) {
          this.currentCard = null;
          this.error = "No cards available to display.";
          return;
        }
        const randomIndex = Math.floor(Math.random() * this.allCards.length);
        this.currentCard = this.allCards[randomIndex];
      },
    },
  };
  </script>
  
  <style scoped>
  /* Add any specific styles if needed */
  .card-image-container img {
    /* Optional: Add subtle shadow or border */
    /* box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2); */
  }
  </style>