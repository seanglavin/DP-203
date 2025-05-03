<template>
    <div class="magic-card-container p-6 max-w-md mx-auto rounded-xl shadow-md overflow-hidden"
      :style="componentStyles.mainContainer"
      >
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
        <div class="card-details p-4 rounded-lg mb-6"
          :style="componentStyles.cardDetails"
          >
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
    function isColorDark(hex) {
      if (!hex) return false;
      let r = 0, g = 0, b = 0;
      // Parse hex (simplified, assumes valid #RRGGBB)
      if (hex.length === 7) {
          r = parseInt(hex.substring(1, 3), 16);
          g = parseInt(hex.substring(3, 5), 16);
          b = parseInt(hex.substring(5, 7), 16);
      } else { return false; } // Handle other formats or return default if needed

      // Calculate YIQ (perceived brightness)
      const yiq = ((r * 299) + (g * 587) + (b * 114)) / 1000;
      return yiq < 128; // Threshold (0-255), lower means darker
  }
    // Helper function to convert hex color to rgba
    function hexToRgba(hex, alpha = 1) {
    if (!hex || typeof hex !== 'string') return null; // Basic validation
    let r = 0, g = 0, b = 0;
    // 3 digits
    if (hex.length === 4) {
      r = parseInt(hex[1] + hex[1], 16);
      g = parseInt(hex[2] + hex[2], 16);
      b = parseInt(hex[3] + hex[3], 16);
    // 6 digits
    } else if (hex.length === 7) {
      r = parseInt(hex[1] + hex[2], 16);
      g = parseInt(hex[3] + hex[4], 16);
      b = parseInt(hex[5] + hex[6], 16);
    } else {
        return null; // Invalid hex format
    }
    if (isNaN(r) || isNaN(g) || isNaN(b)) return null; // Check parsing result

    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
  }

  // Define MTG color mappings
  const mtgColorMap = {
    'W': { primary: '#F8F6D8', secondary: '#FFFCEC' }, // Parchment -> Lighter Parchment
    'U': { primary: '#0E68AB', secondary: '#AAE0FA' }, // Blue -> Sky Blue
    'B': { primary: '#555555', secondary: '#AAAAAA' }, // Dark Gray -> Medium Gray
    'R': { primary: '#D3202A', secondary: '#F9AAA5' }, // Red -> Light Red/Pinkish
    'G': { primary: '#00733E', secondary: '#97D7B3' }, // Green -> Light Green
    'C': { primary: '#AAAAAA', secondary: '#CCCCCC' }, // Gray -> Lighter Gray
    'M': { primary: '#D4AF37', secondary: '#EAE0B2' }, // Gold -> Pale Gold
  };
  // const defaultContainerBg = 'var(--secondary-light)'; // Your original background
  // Define default fallbacks if needed (using Tailwind colors for example)
  const defaultPrimaryBg = '#E5E7EB'; // Tailwind gray-200
  const defaultSecondaryBg = '#F3F4F6'; // Tailwind gray-100
  const defaultTextColor = '#1F2937'; // Default dark text (Tailwind gray-800)
  const lightTextColor = '#F9FAFB'; // Light text for dark backgrounds (Tailwind gray-50)

  export default {
    name: 'MagicCardDisplay',
    data() {
      return {
        loading: true,
        error: null,
        allCards: [],
        currentCard: null,
      };
    },
    computed: {
      /**
       * Determines the primary color code (W, U, B, R, G, C, M) for the current card.
       */
      cardPrimaryColorCode() {
        if (!this.currentCard || !this.currentCard.color_identity || this.currentCard.color_identity.length === 0) {
          return 'C'; // Colorless
        }
        if (this.currentCard.color_identity.length === 1) {
          return this.currentCard.color_identity[0]; // Single color
        }
        return 'M'; // Multicolor
      },
      /**
       * Generates style objects for container and details section using primary/secondary map.
       */
       componentStyles() {
        const colorCode = this.cardPrimaryColorCode;
        // Get the color object, or provide defaults
        const colors = mtgColorMap[colorCode] || { primary: defaultPrimaryBg, secondary: defaultSecondaryBg };

        // --- Main Container Background (using primary color with gradient) ---
        const centerColor = hexToRgba(colors.primary, 0.85);
        const edgeColor = hexToRgba(colors.primary, 1);
        const mainBackgroundStyle = centerColor && edgeColor
          ? `radial-gradient(circle at center, ${centerColor} 0%, ${edgeColor} 80%)`
          : colors.primary; // Fallback to solid primary if rgba fails

        // --- Details Section Background & Text Color (using secondary color) ---
        const detailsBgHex = colors.secondary;
        const detailsTextColor = isColorDark(detailsBgHex) ? lightTextColor : defaultTextColor;

        return {
          mainContainer: {
            background: mainBackgroundStyle,
            transition: 'background 0.5s ease',
          },
          cardDetails: {
            backgroundColor: detailsBgHex,
            color: detailsTextColor,
            transition: 'background-color 0.5s ease, color 0.5s ease',
          }
        };
      }
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
          const response = await fetch('http://localhost:8000/api/mtg/scryfall/cards/daily1000');
  
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