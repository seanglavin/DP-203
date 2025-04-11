<template>
  <div class="pet-game-container p-6 max-w-lg mx-auto bg-white rounded-xl shadow-md">
    <h2 class="text-2xl font-bold text-center mb-4">Pet Name Guessing Game</h2>
    
    <!-- Game state display -->
    <div class="text-center mb-4">
      <p class="text-lg">Score: {{ score }}</p>
      <p v-if="feedback" :class="feedbackClass">{{ feedback }}</p>
    </div>

    <!-- Pet image -->
    <div class="pet-image-container mb-6">
      <div v-if="loading" class="flex justify-center items-center h-64">
        <div class="text-center">
          <p class="text-gray-500 mb-2">Loading pet image...</p>
          <div class="w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto"></div>
        </div>
      </div>
      <img 
        v-else-if="currentPet.photo_url" 
        :src="currentPet.photo_url" 
        :alt="hasGuessed ? currentPet.name : 'Mystery pet'" 
        class="w-full h-64 object-cover rounded-lg"
      >
      <div v-else class="flex justify-center items-center h-64 bg-gray-200 rounded-lg">
        <p class="text-gray-500">No image available</p>
      </div>
    </div>

    <!-- Pet information (shown after guessing) -->
    <div v-if="hasGuessed" class="mb-6 p-4 bg-gray-100 rounded-lg">
      <h3 class="font-bold text-lg mb-2">{{ currentPet.name }}</h3>
      <p><span class="font-semibold">Type:</span> {{ currentPet.type }}</p>
      <p v-if="currentPet.primary_breed"><span class="font-semibold">Breed:</span> {{ currentPet.primary_breed }}</p>
      <p><span class="font-semibold">Age:</span> {{ currentPet.age }}</p>
      <p v-if="currentPet.description" class="mt-2 text-sm">
        {{ truncateDescription(currentPet.description) }}
      </p>
    </div>

    <!-- Name options -->
    <div class="grid grid-cols-2 gap-4 mb-6">
      <button
        v-for="(name, index) in nameOptions"
        :key="index"
        @click="makeGuess(name)"
        :disabled="hasGuessed"
        class="py-2 px-4 bg-blue-500 text-white rounded hover:bg-blue-600 disabled:opacity-50 transition"
      >
        {{ name }}
      </button>
    </div>

    <!-- Next/Restart button -->
    <div class="text-center">
      <button
        @click="nextPet"
        class="py-2 px-8 bg-green-500 text-white rounded hover:bg-green-600 transition"
      >
        {{ hasGuessed ? 'Next Pet' : 'Skip This Pet' }}
      </button>
    </div>
    
    <!-- Filter options -->
    <div class="mt-6">
      <details class="bg-gray-100 rounded-lg p-4">
        <summary class="font-semibold cursor-pointer">Game Options</summary>
        <div class="mt-4 grid grid-cols-2 gap-4">
          <div>
            <label class="block text-sm font-medium text-gray-700 mb-1">Pet Type</label>
            <select v-model="filters.type" class="w-full p-2 border rounded">
              <option value="">Any Type</option>
              <option value="Dog">Dogs</option>
              <option value="Cat">Cats</option>
              <option value="Rabbit">Rabbits</option>
              <option value="Bird">Birds</option>
            </select>
          </div>
          <div>
            <label class="block text-sm font-medium text-gray-700 mb-1">Pet Age</label>
            <select v-model="filters.age" class="w-full p-2 border rounded">
              <option value="">Any Age</option>
              <option value="Baby">Baby</option>
              <option value="Young">Young</option>
              <option value="Adult">Adult</option>
              <option value="Senior">Senior</option>
            </select>
          </div>
        </div>
        <div class="mt-4">
          <button 
            @click="applyFilters" 
            class="w-full py-2 px-4 bg-blue-500 text-white rounded hover:bg-blue-600 transition"
          >
            Apply Filters & Restart Game
          </button>
        </div>
      </details>
    </div>
  </div>
</template>

<script>
export default {
  name: 'PetNameGame',
  data() {
    return {
      score: 0,
      hasGuessed: false,
      feedback: '',
      feedbackClass: '',
      loading: true,
      currentPet: {},
      nameOptions: [],
      gameBoards: [],
      currentBoardIndex: 0,
      currentPetIndex: 1, // Changed to 1 since pets are now indexed starting with 1
      filters: {
        type: '',
        age: ''
      }
    };
  },
  mounted() {
    this.fetchGameBoards();
  },
  methods: {
    async fetchGameBoards() {
      this.loading = true;
      this.feedback = '';
      
      try {
        // Construct query parameters based on filters
        const queryParams = new URLSearchParams();
        if (this.filters.type) queryParams.append('type', this.filters.type);
        if (this.filters.age) queryParams.append('age', this.filters.age);
        
        const queryString = queryParams.toString();
        const endpoint = `http://localhost:8000/api/petfinder/pets/gameboards${queryString ? '?' + queryString : ''}`;

        console.log("Fetching from:", endpoint); // Log the endpoint URL

        const response = await fetch(endpoint);
        console.log("Raw response:", response); // Log the raw response object

        const data = await response.json();
        console.log("Fetched data:", data); // Log the parsed JSON data
        
        if (data.gameboards && data.gameboards.length > 0) {
          this.gameBoards = data.gameboards;
          console.log("Assigned gameBoards:", this.gameBoards); // Log assigned gameBoards

          this.currentBoardIndex = 0;
          this.currentPetIndex = 1; // Start with pet1
          this.loadCurrentPet();
        } else {
          this.feedback = 'No pets found with the current filters. Please try different options.';
          this.feedbackClass = 'text-red-600 font-bold';
          this.loading = false;
        }
      } catch (error) {
        console.error('Error fetching game boards:', error);
        this.feedback = 'Error loading pets. Please try again later.';
        this.feedbackClass = 'text-red-600 font-bold';
        this.loading = false;
      }
    },
    
    loadCurrentPet() {
      this.loading = true;
      this.hasGuessed = false;
      this.feedback = '';
      
      // Check if we have a current game board
      if (!this.gameBoards[this.currentBoardIndex]) {
        this.fetchGameBoards();
        return;
      }
      
      // Get the current pet using the pet key (pet1, pet2, etc.)
      const currentBoard = this.gameBoards[this.currentBoardIndex];
      const petKey = `pet${this.currentPetIndex}`;
      this.currentPet = currentBoard[petKey];
      
      console.log("Current pet key:", petKey);
      console.log("Current pet object:", this.currentPet); // Log the current pet

      if (!this.currentPet) {
        // Move to next board or fetch new boards if at the end
        if (this.currentBoardIndex < this.gameBoards.length - 1) {
          this.currentBoardIndex++;
          this.currentPetIndex = 1; // Reset to pet1 for the next board
          this.loadCurrentPet();
        } else {
          this.fetchGameBoards();
        }
        return;
      }
      
      // Generate name options
      this.generateNameOptions();
      this.loading = false;
    },
    
    generateNameOptions() {
      // Always include the correct name
      const options = [this.currentPet.name];
      
      // Add random names from other pets in different game boards
      const allPetNames = this.getAllPetNames();
      
      while (options.length < 4 && allPetNames.length > 0) {
        const randomIndex = Math.floor(Math.random() * allPetNames.length);
        const randomName = allPetNames[randomIndex];
        
        if (!options.includes(randomName) && randomName !== this.currentPet.name) {
          options.push(randomName);
        }
        
        // Remove the name to avoid duplicates
        allPetNames.splice(randomIndex, 1);
      }
      
      // If we don't have enough names, generate some common pet names
      const commonPetNames = ['Max', 'Bella', 'Luna', 'Charlie', 'Lucy', 'Cooper', 
                             'Bailey', 'Daisy', 'Sadie', 'Lola', 'Buddy', 'Molly', 
                             'Stella', 'Tucker', 'Bentley', 'Zoey', 'Harley', 'Rocky'];
      
      while (options.length < 4) {
        const randomName = commonPetNames[Math.floor(Math.random() * commonPetNames.length)];
        if (!options.includes(randomName)) {
          options.push(randomName);
        }
      }
      
      // Shuffle options
      this.nameOptions = this.shuffleArray(options);
    },
    
    getAllPetNames() {
      // Collect all pet names from all game boards
      const names = [];
      this.gameBoards.forEach(board => {
        // Iterate through each pet in the board (pet1, pet2, etc.)
        Object.keys(board).forEach(petKey => {
          const pet = board[petKey];
          if (pet && pet.name && pet.name !== this.currentPet.name) {
            names.push(pet.name);
          }
        });
      });
      return names;
    },
    
    shuffleArray(array) {
      const newArray = [...array];
      for (let i = newArray.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [newArray[i], newArray[j]] = [newArray[j], newArray[i]];
      }
      return newArray;
    },
    
    makeGuess(name) {
      this.hasGuessed = true;
      
      if (name === this.currentPet.name) {
        this.feedback = 'Correct! This pet is indeed named ' + name;
        this.feedbackClass = 'text-green-600 font-bold';
        this.score += 10;
      } else {
        this.feedback = `Incorrect. The pet's name is ${this.currentPet.name}`;
        this.feedbackClass = 'text-red-600 font-bold';
      }
    },
    
    nextPet() {
      this.currentPetIndex++;
      
      // Check if we need to move to the next board
      const currentBoard = this.gameBoards[this.currentBoardIndex];
      const maxPets = Object.keys(currentBoard).filter(key => key.startsWith('pet')).length;
      
      if (this.currentPetIndex > maxPets) {
        this.currentPetIndex = 1; // Reset to pet1
        this.currentBoardIndex++;
        
        // Check if we're out of boards
        if (this.currentBoardIndex >= this.gameBoards.length) {
          this.fetchGameBoards();
          return;
        }
      }
      
      this.loadCurrentPet();
    },
    
    applyFilters() {
      this.score = 0; // Reset score when applying new filters
      this.fetchGameBoards();
    },
    
    truncateDescription(description) {
      if (!description) return '';
      return description.length > 150 ? description.substring(0, 150) + '...' : description;
    }
  }
};
</script>

<style scoped>
.pet-game-container {
  transition: all 0.3s ease;
}
</style>