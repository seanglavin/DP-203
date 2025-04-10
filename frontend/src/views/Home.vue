<template>
  <div class="home">
    <!-- <h2 class="text-3xl font-bold mb-6">Choose a Game</h2> -->
    
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
      <!-- Game Card for Pet Name Guessing -->
      <div class="game-card bg-white p-6 rounded-xl shadow-md hover:shadow-lg transition">
        <h3 class="text-xl font-bold mb-2">Petfinder Name Game</h3>
        <p class="mb-4">Can you guess the correct names of these adorable pets?</p>
        <router-link 
          to="/games/pet-name-game" 
          class="block text-center py-2 px-4 bg-blue-500 text-white rounded hover:bg-blue-600 transition"
        >
          Play Now
        </router-link>
      </div>
      
      <!-- Additional game cards would go here -->
      <!-- <div class="game-card bg-white p-6 rounded-xl shadow-md hover:shadow-lg transition">
        <h3 class="text-xl font-bold mb-2">Pet Breed Quiz</h3>
        <p class="mb-4">Test your knowledge of different pet breeds!</p>
        <router-link 
          to="/games/breed-quiz" 
          class="block text-center py-2 px-4 bg-blue-500 text-white rounded hover:bg-blue-600 transition"
        >
          Coming Soon
        </router-link>
      </div> -->
    </div>
  </div>
</template>

<script>
export default {
  name: 'Home'
}
</script>